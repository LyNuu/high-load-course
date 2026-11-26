package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

/**
 * Кейс 7: подтверждённые HTTP 408 можно повторить новым transactionId.
 * Нельзя повторять InterruptedIOException: итог первой операции неизвестен.
 *
 * Параметр timeout внешнего провайдера принимает java.time.Duration
 * в ISO-8601 формате (например PT1.591S), а не целое число миллисекунд.
 * Не путать его с клиентским OkHttp call.timeout().
 */
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val serverTimeoutRetryEnabled: Boolean = false
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val emptyBody = RequestBody.create(null, ByteArray(0))
        private val mapper = ObjectMapper().registerKotlinModule()

        private const val MAX_ATTEMPTS = 3
        private const val MIN_ATTEMPT_MS = 1_300L
        private const val MAX_ATTEMPT_MS = 1_800L
        private const val FINAL_RESERVE_MS = 250L
        private const val PROVIDER_MARGIN_MS = 100L
        private const val RESERVE_MS = 300L
        private const val ACC7_SPACING_MS = 135L

        // Провайдер принимает этот параметр, тип значения: java.time.Duration.
        private const val PROVIDER_TIMEOUT_PARAMETER = "timeout"
    }

    private val accountName = properties.accountName
    private val enabledRetries = serverTimeoutRetryEnabled && accountName == "acc-7"
    private val providerBudgetMs = maxOf(1L, properties.averageProcessingTime.toMillis()) + PROVIDER_MARGIN_MS

    // Запрещаем неявные повторные отправки HTTP-клиентом в режиме кейса 7.
    private val client = OkHttpClient.Builder()
        .retryOnConnectionFailure(!enabledRetries)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(
        properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1)
    )
    private val pacedLimiter = if (enabledRetries) PacedLimiter(ACC7_SPACING_MS) else null
    private val parallelWindow = OngoingWindow(properties.parallelRequests)

    private val outgoingCounter = Metrics.counter("shop.payments.outgoing", "account", accountName)
    private val retryCounter = Metrics.counter("payment.provider.retries", "account", accountName)
    private val confirmedTimeoutCounter = Metrics.counter("payment.provider.case7.confirmed.timeouts", "account", accountName)
    private val skippedRetryCounter = Metrics.counter("payment.provider.case7.skipped.retries", "account", accountName)
    private val networkTimeoutCounter = Metrics.counter("payment.provider.case7.network.timeouts", "account", accountName)
    private val providerHttpTimer = Timer.builder("shop.payments.provider.http")
        .tag("account", accountName)
        .publishPercentileHistogram()
        .register(Metrics.globalRegistry)

    private data class AttemptResult(
        val value: ExternalSysResponse,
        val retryable: Boolean
    )

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        var lastTxId = UUID.randomUUID()
        val maxAttempts = if (enabledRetries) MAX_ATTEMPTS else 1
        var lastResult: ExternalSysResponse? = null

        try {
            for (attempt in 1..maxAttempts) {
                val remainingBeforeSubmission = deadline - now()
                if (attempt > 1 && remainingBeforeSubmission < MIN_ATTEMPT_MS + FINAL_RESERVE_MS) {
                    skippedRetryCounter.increment()
                    logger.warn("[CASE7] RETRY_SKIPPED paymentId={} remainingMs={} reason=deadline", paymentId, remainingBeforeSubmission)
                    break
                }

                if (attempt > 1) lastTxId = UUID.randomUUID()
                recordSubmission(paymentId, lastTxId, paymentStartedAt, deadline)

                val attemptResult = try {
                    performAttempt(
                        paymentId = paymentId,
                        transactionId = lastTxId,
                        amount = amount,
                        deadline = deadline,
                        attempt = attempt,
                        attemptsLeft = maxAttempts - attempt + 1
                    )
                } catch (e: InterruptedIOException) {
                    networkTimeoutCounter.increment()
                    logger.warn(
                        "[CASE7] NETWORK_TIMEOUT_NO_RETRY account={} paymentId={} attempt={} remainingMs={} error={}",
                        accountName, paymentId, attempt, deadline - now(), e.javaClass.simpleName
                    )
                    lastResult = failure(paymentId, lastTxId, "Request timeout.")
                    break
                }

                lastResult = attemptResult.value
                if (!attemptResult.retryable || attempt == maxAttempts) break

                val remaining = deadline - now()
                if (remaining < MIN_ATTEMPT_MS + FINAL_RESERVE_MS) {
                    skippedRetryCounter.increment()
                    logger.warn("[CASE7] RETRY_SKIPPED paymentId={} remainingMs={} reason=deadline", paymentId, remaining)
                    break
                }

                logger.info(
                    "[CASE7] HTTP_408_RETRY account={} paymentId={} afterAttempt={} remainingMs={}",
                    accountName, paymentId, attempt, remaining
                )
            }
        } catch (e: Exception) {
            if (e is InterruptedException) Thread.currentThread().interrupt()
            logger.error("[CASE7] PAYMENT_EXCEPTION account={} paymentId={}", accountName, paymentId, e)
            if (lastResult == null) lastResult = failure(paymentId, lastTxId, e.message ?: "Payment error")
        }

        val result = lastResult ?: failure(paymentId, lastTxId, "No provider attempt completed")
        if (!result.result) {
            val message = result.message.orEmpty()
            val failureType = when {
                message.contains("rate limiter", ignoreCase = true) -> "rate_wait"
                message.contains("deadline", ignoreCase = true) ||
                        message.contains("Not enough time", ignoreCase = true) -> "deadline"
                message.contains("Request timeout", ignoreCase = true) -> "network_timeout"
                message.contains("Invalid provider response", ignoreCase = true) -> "invalid_response"
                message.trim().equals("Timeout", ignoreCase = true) -> "server_timeout"
                else -> "provider_failure"
            }
            Metrics.counter(
                "payment.provider.case7.final.failures",
                "account", accountName,
                "reason", failureType
            ).increment()
        }
        logger.info(
            "[{}] Payment processed: paymentId={}, succeeded={}, reason={}",
            accountName, paymentId, result.result, result.message
        )
        // Одно финальное событие Processing после всех попыток.
        paymentESService.update(paymentId) {
            it.logProcessing(result.result, now(), lastTxId, reason = result.message)
        }
    }

    private fun recordSubmission(
        paymentId: UUID,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        val started = System.nanoTime()
        paymentESService.update(paymentId) {
            it.logSubmission(
                success = true,
                transactionId,
                now(),
                Duration.ofMillis((now() - paymentStartedAt).coerceAtLeast(0L))
            )
        }
        val elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
        if (elapsed > 200L) {
            logger.warn(
                "[CASE7] SLOW_DB_SUBMISSION paymentId={} ms={} remainingMs={}",
                paymentId, elapsed, deadline - now()
            )
        }
    }

    private fun performAttempt(
        paymentId: UUID,
        transactionId: UUID,
        amount: Int,
        deadline: Long,
        attempt: Int,
        attemptsLeft: Int
    ): AttemptResult {
        if (deadline <= now()) {
            return AttemptResult(failure(paymentId, transactionId, "Payment deadline expired"), false)
        }
        var slotAcquired = false
        try {
            // Ограничиваем максимальный параллелизм для аккаунта.
            parallelWindow.acquire()
            slotAcquired = true

            val remaining = deadline - now()
            val minimumNeeded = if (enabledRetries) MIN_ATTEMPT_MS else providerBudgetMs
            val waitBudget = remaining - minimumNeeded - RESERVE_MS
            if (waitBudget <= 0L || !acquireRatePermit(waitBudget)) {
                return AttemptResult(failure(paymentId, transactionId, "Insufficient time for rate limiter"), false)
            }

            val afterRateMs = deadline - now()
            if (afterRateMs <= FINAL_RESERVE_MS + minimumNeeded) {
                return AttemptResult(failure(paymentId, transactionId, "Not enough time to process payment"), false)
            }

            // Не делим бюджет поровну на 3 попытки: это давало ~1.55 с первой
            // попытке, повышая вероятность HTTP 408 и создавая лишние retries.
            // Берём до 1.8 с СЕЙЧАС, оставляя на каждую будущую попытку хотя бы
            // 1.3 с и 135 мс на исходящий rate limiter. Не выходим за deadline.
            val perAttemptMs = if (enabledRetries) {
                val futureMinimum = (attemptsLeft - 1) *
                        (MIN_ATTEMPT_MS + ACC7_SPACING_MS)
                val available = afterRateMs - FINAL_RESERVE_MS - futureMinimum
                if (available < MIN_ATTEMPT_MS) {
                    logger.warn(
                        "[CASE7] ATTEMPT_SKIPPED paymentId={} attempt={} availableMs={} remainingMs={}",
                        paymentId, attempt, available, afterRateMs
                    )
                    return AttemptResult(
                        failure(paymentId, transactionId, "Not enough time to process payment"), false
                    )
                }
                available.coerceAtMost(MAX_ATTEMPT_MS)
            } else {
                0L
            }

            val requestUrl = buildString {
                append("http://").append(paymentProviderHostPort).append("/external/process")
                append("?serviceName=").append(properties.serviceName)
                append("&token=").append(token)
                append("&accountName=").append(accountName)
                append("&transactionId=").append(transactionId)
                append("&paymentId=").append(paymentId)
                append("&amount=").append(amount)
                if (enabledRetries) {
                    // Передаём бюджет ПРОВАЙДЕРУ, а не ограничиваем им OkHttp.
                    // Duration.toString() выдаёт формат ISO-8601: PT1.591S.
                    append("&").append(PROVIDER_TIMEOUT_PARAMETER).append("=").append(Duration.ofMillis(perAttemptMs).toString())
                }
            }

            val request = Request.Builder().url(requestUrl).post(emptyBody).build()
            if (attempt > 1) retryCounter.increment()
            outgoingCounter.increment()

            val started = System.nanoTime()
            try {
                return client.newCall(request).execute().use { http ->
                    val raw = http.body?.string()
                    val response = try {
                        mapper.readValue(raw, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[CASE7] RESPONSE_PARSE_FAILED paymentId={} status={} body={}", paymentId, http.code, raw, e)
                        return@use AttemptResult(failure(paymentId, transactionId, "Invalid provider response"), false)
                    }

                    val canRetry = if (enabledRetries && http.code == 408 && !response.result) {
                        try {
                            val json = mapper.readTree(raw)
                            json.path("result").isBoolean &&
                                    !json.path("result").asBoolean() &&
                                    json.path("message").asText("").trim().equals("Timeout", true) &&
                                    json.path("paymentId").asText("") == paymentId.toString() &&
                                    json.path("transactionId").asText("") == transactionId.toString()
                        } catch (_: Exception) {
                            false
                        }
                    } else false

                    if (http.code == 408) {
                        if (canRetry) confirmedTimeoutCounter.increment()
                        logger.info(
                            "[CASE7] PROVIDER_408 account={} paymentId={} retryable={} remainingMs={} msg={}",
                            accountName, paymentId, canRetry, deadline - now(), response.message
                        )
                    }
                    AttemptResult(response, canRetry)
                }
            } finally {
                val elapsedNs = System.nanoTime() - started
                providerHttpTimer.record(elapsedNs, TimeUnit.NANOSECONDS)
                val httpMs = TimeUnit.NANOSECONDS.toMillis(elapsedNs)
                if (httpMs > 2500L) {
                    logger.warn("[CASE7] SLOW_PROVIDER account={} paymentId={} attempt={} httpMs={} remainingMs={}",
                        accountName, paymentId, attempt, httpMs, deadline - now())
                }
            }
        } finally {
            if (slotAcquired) parallelWindow.release()
        }
    }

    private fun acquireRatePermit(timeoutMs: Long): Boolean =
        pacedLimiter?.acquire(timeoutMs) ?: rateLimiter.tickWithTimeout(timeoutMs)

    private fun failure(paymentId: UUID, transactionId: UUID, reason: String) =
        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, reason)

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

private class PacedLimiter(spacingMs: Long) {
    private val lock = ReentrantLock(true)
    private val spacingNs = TimeUnit.MILLISECONDS.toNanos(spacingMs)
    private var nextNs = 0L

    fun acquire(timeoutMs: Long): Boolean {
        val started = System.nanoTime()
        val maxWait = timeoutMs.coerceAtLeast(0L)
        if (!lock.tryLock(maxWait, TimeUnit.MILLISECONDS)) return false
        try {
            val budgetNs = TimeUnit.MILLISECONDS.toNanos(maxWait) - (System.nanoTime() - started)
            if (budgetNs <= 0) return false
            val waitNs = (nextNs - System.nanoTime()).coerceAtLeast(0)
            if (waitNs >= budgetNs) return false
            if (waitNs > 0) TimeUnit.NANOSECONDS.sleep(waitNs)
            nextNs = System.nanoTime() + spacingNs
            return true
        } finally {
            lock.unlock()
        }
    }
}

fun now(): Long = System.currentTimeMillis()
