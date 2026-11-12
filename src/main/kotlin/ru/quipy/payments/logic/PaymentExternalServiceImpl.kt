
package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Metrics
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService:
    EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(
            PaymentExternalSystemAdapterImpl::class.java
        )

        private val emptyBody = RequestBody.create(null, ByteArray(0))
        private val mapper = ObjectMapper().registerKotlinModule()

        private const val PROVIDER_MARGIN_MS = 100L
        private const val RESERVE_MS = 300L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // acc-8: 700 + 100 = 800 мс для провайдера.
    // acc-23: 1000 + 100 = 1100 мс.
    private val providerBudgetMs =
        maxOf(
            1L,
            properties.averageProcessingTime.toMillis()
        ) + PROVIDER_MARGIN_MS

    private val client = OkHttpClient.Builder().build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )

    private val parallelWindow = OngoingWindow(parallelRequests)

    private val outgoingCounter = Metrics.counter(
        "shop.payments.outgoing",
        "account",
        accountName
    )

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        val transactionId = UUID.randomUUID()

        // Обязательно регистрируем Submission в Event Sourcing.
        val submissionStarted = System.nanoTime()

        try {
            paymentESService.update(paymentId) {
                it.logSubmission(
                    success = true,
                    transactionId,
                    now(),
                    Duration.ofMillis(
                        (now() - paymentStartedAt).coerceAtLeast(0L)
                    )
                )
            }
        } catch (e: Exception) {
            logger.error(
                "[PAYMENT-DIAG] [DB_SUBMISSION_FAILED] paymentId={}",
                paymentId,
                e
            )
            throw e
        } finally {
            val dbMs = TimeUnit.NANOSECONDS.toMillis(
                System.nanoTime() - submissionStarted
            )

            if (dbMs > 200L) {
                logger.warn(
                    "[PAYMENT-DIAG] [SLOW_DB_SUBMISSION] " +
                            "paymentId={} dbTimeMs={} remainingMs={}",
                    paymentId,
                    dbMs,
                    deadline - now()
                )
            }
        }

        // Отделяем ошибки провайдера от ошибок сохранения в БД.
        val result: ExternalSysResponse = try {
            callProvider(
                paymentId,
                amount,
                transactionId,
                deadline
            )
        } catch (e: SocketTimeoutException) {
            logger.error(
                "[PAYMENT-DIAG] [NETWORK_TIMEOUT] paymentId={}",
                paymentId,
                e
            )

            failedResponse(
                paymentId,
                transactionId,
                "Request timeout."
            )
        } catch (e: Exception) {
            if (e is InterruptedException) {
                Thread.currentThread().interrupt()
            }

            logger.error(
                "[PAYMENT-DIAG] [PAYMENT_EXCEPTION] paymentId={}",
                paymentId,
                e
            )

            failedResponse(
                paymentId,
                transactionId,
                e.message ?: "Payment error"
            )
        }

        if (result.result) {
            logger.info(
                "[{}] Payment processed: paymentId={}, succeeded=true",
                accountName,
                paymentId
            )
        } else {
            logger.warn(
                "[{}] Payment processed: paymentId={}, " +
                        "succeeded=false, reason={}",
                accountName,
                paymentId,
                result.message
            )
        }

        // Сохраняем результат отдельно от сетевого вызова.
        // Ошибка БД не должна превращать успешную оплату
        // в ложный отрицательный результат.
        try {
            paymentESService.update(paymentId) {
                it.logProcessing(
                    result.result,
                    now(),
                    transactionId,
                    reason = result.message
                )
            }
        } catch (e: Exception) {
            logger.error(
                "[PAYMENT-DIAG] [DB_PROCESSING_FAILED] " +
                        "paymentId={} providerSucceeded={}",
                paymentId,
                result.result,
                e
            )
            throw e
        }
    }

    private fun callProvider(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ): ExternalSysResponse {

        // Проверка deadline до ожидания слота.
        if (deadline <= now()) {
            logger.warn(
                "[PAYMENT-DIAG] [DEADLINE_BEFORE_SLOT] " +
                        "paymentId={}",
                paymentId
            )

            return failedResponse(
                paymentId,
                transactionId,
                "Payment deadline expired"
            )
        }

        var windowAcquired = false

        try {
            val slotStarted = System.nanoTime()

            parallelWindow.acquire()
            windowAcquired = true

            val slotWaitMs = TimeUnit.NANOSECONDS.toMillis(
                System.nanoTime() - slotStarted
            )

            if (slotWaitMs > 100L) {
                logger.warn(
                    "[PAYMENT-DIAG] [SLOW_PARALLEL_SLOT] " +
                            "paymentId={} slotWaitMs={}",
                    paymentId,
                    slotWaitMs
                )
            }

            // Рассчитываем оставшееся время.
            val remainingMs = deadline - now()

            val waitBudgetMs =
                remainingMs - providerBudgetMs - RESERVE_MS

            // Ждём разрешения исходящего лимитера
            // только в пределах доступного бюджета.
            if (
                waitBudgetMs <= 0L ||
                !rateLimiter.tickWithTimeout(waitBudgetMs)
            ) {
                logger.warn(
                    "[PAYMENT-DIAG] [RATE_WAIT_EXCEEDED] " +
                            "paymentId={} remainingMs={} " +
                            "waitBudgetMs={} providerBudgetMs={}",
                    paymentId,
                    remainingMs,
                    waitBudgetMs,
                    providerBudgetMs
                )

                return failedResponse(
                    paymentId,
                    transactionId,
                    "Insufficient time for rate limiter"
                )
            }

            // Повторно проверяем время после ожидания.
            val afterWaitMs = deadline - now()

            if (afterWaitMs <= providerBudgetMs + RESERVE_MS) {
                logger.warn(
                    "[PAYMENT-DIAG] [DEADLINE_BUDGET_EXCEEDED] " +
                            "paymentId={} remainingMs={}",
                    paymentId,
                    afterWaitMs
                )

                return failedResponse(
                    paymentId,
                    transactionId,
                    "Not enough time to process payment"
                )
            }

            val request = Request.Builder().run {
                url(
                    "http://$paymentProviderHostPort/external/process" +
                            "?serviceName=$serviceName" +
                            "&token=$token" +
                            "&accountName=$accountName" +
                            "&transactionId=$transactionId" +
                            "&paymentId=$paymentId" +
                            "&amount=$amount"
                )

                post(emptyBody)
            }.build()

            outgoingCounter.increment()

            return client.newCall(request).execute().use { response ->
                val responseText = response.body?.string()

                try {
                    mapper.readValue(
                        responseText,
                        ExternalSysResponse::class.java
                    )
                } catch (e: Exception) {
                    logger.error(
                        "[PAYMENT-DIAG] [RESPONSE_PARSE_FAILED] " +
                                "paymentId={} status={}",
                        paymentId,
                        response.code,
                        e
                    )

                    failedResponse(
                        paymentId,
                        transactionId,
                        e.message ?: "Invalid provider response"
                    )
                }
            }

        } finally {
            if (windowAcquired) {
                parallelWindow.release()
            }
        }
    }

    private fun failedResponse(
        paymentId: UUID,
        transactionId: UUID,
        reason: String
    ): ExternalSysResponse {
        return ExternalSysResponse(
            transactionId.toString(),
            paymentId.toString(),
            false,
            reason
        )
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

fun now(): Long = System.currentTimeMillis()
