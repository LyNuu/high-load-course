
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

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<
            UUID,
            PaymentAggregate,
            PaymentAggregateState
            >,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(
            PaymentExternalSystemAdapterImpl::class.java
        )

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        // Для теста №1: processingTimeMillis = 2500.
        // Оценка времени ответа провайдера и запас на БД.
        private const val PROVIDER_BUDGET_MS = 1100L
        private const val RESERVE_MS = 300L
        private const val MAX_RATE_WAIT_MS = 500L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime =
        properties.averageProcessingTime

    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

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
        logger.info(
            "[$accountName] Submitting payment request for $paymentId"
        )

        val transactionId = UUID.randomUUID()

        // Обязательно фиксируем отправку платежа в Event Sourcing.
        paymentESService.update(paymentId) {
            it.logSubmission(
                success = true,
                transactionId,
                now(),
                Duration.ofMillis(now() - paymentStartedAt)
            )
        }

        logger.info(
            "[$accountName] Submit: $paymentId, txId: $transactionId"
        )

        var windowAcquired = false

        try {
            // Сохраняем ограничение параллельных запросов.
            parallelWindow.acquire()
            windowAcquired = true

            // Считаем оставшееся время.
            val remainingMs = deadline - now()

            // Сколько можно потратить на ожидание лимитера,
            // чтобы оставить время на HTTP и сохранение результата.
            val waitBudgetMs = remainingMs - PROVIDER_BUDGET_MS - RESERVE_MS

            if (
                waitBudgetMs <= 0L ||
                !rateLimiter.tickWithTimeout(waitBudgetMs)
            ) {
                logger.warn(
                    "[PAYMENT-DIAG] [RATE_WAIT_EXCEEDED] " +
                            "paymentId={} remainingMs={} waitBudgetMs={}",
                    paymentId,
                    remainingMs,
                    waitBudgetMs
                )

                paymentESService.update(paymentId) {
                    it.logProcessing(
                        false,
                        now(),
                        transactionId,
                        reason = "Insufficient time for rate limiter"
                    )
                }

                return
            }

            // После ожидания дополнительно проверяем deadline.
            val remainingAfterWaitMs = deadline - now()

            if (
                remainingAfterWaitMs <=
                PROVIDER_BUDGET_MS + RESERVE_MS
            ) {
                logger.warn(
                    "[PAYMENT-DIAG] [DEADLINE_BUDGET_EXCEEDED] " +
                            "paymentId={} remainingMs={}",
                    paymentId,
                    remainingAfterWaitMs
                )

                paymentESService.update(paymentId) {
                    it.logProcessing(
                        false,
                        now(),
                        transactionId,
                        reason = "Not enough time to process payment"
                    )
                }

                return
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

            client.newCall(request).execute().use { response ->
                val responseText = response.body?.string()

                val body = try {
                    mapper.readValue(
                        responseText,
                        ExternalSysResponse::class.java
                    )
                } catch (e: Exception) {
                    logger.error(
                        "[$accountName] [RESPONSE_PARSE_FAILED] " +
                                "paymentId=$paymentId txId=$transactionId " +
                                "status=${response.code}",
                        e
                    )

                    ExternalSysResponse(
                        transactionId.toString(),
                        paymentId.toString(),
                        false,
                        e.message
                    )
                }

                logger.info(
                    "[$accountName] Payment processed: " +
                            "paymentId=$paymentId, " +
                            "succeeded=${body.result}, " +
                            "message=${body.message}"
                )

                paymentESService.update(paymentId) {
                    it.logProcessing(
                        body.result,
                        now(),
                        transactionId,
                        reason = body.message
                    )
                }
            }

        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error(
                        "[$accountName] [NETWORK_TIMEOUT] " +
                                "paymentId=$paymentId",
                        e
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(
                            false,
                            now(),
                            transactionId,
                            reason = "Request timeout."
                        )
                    }
                }

                else -> {
                    logger.error(
                        "[$accountName] [PAYMENT_EXCEPTION] " +
                                "paymentId=$paymentId",
                        e
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(
                            false,
                            now(),
                            transactionId,
                            reason = e.message
                        )
                    }
                }
            }
        } finally {
            if (windowAcquired) {
                parallelWindow.release()
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
