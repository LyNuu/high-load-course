package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okio.BufferedSink
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
        private val oneShotEmptyBody = object : RequestBody() {
            override fun contentType() = null
            override fun contentLength() = 0L
            override fun isOneShot() = true
            override fun writeTo(sink: BufferedSink) = Unit
        }
        private val mapper = ObjectMapper().registerKotlinModule()
        private val timeoutResponseReader = mapper.reader()
            .with(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
            .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

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
    private val minimumAttemptMs = if (enabledRetries) MIN_ATTEMPT_MS else 1L
    private val waitReserveMs = if (enabledRetries) RESERVE_MS else 0L
    private val finalReserveMs = if (enabledRetries) FINAL_RESERVE_MS else 0L

    // Запрещаем неявные повторные отправки HTTP-клиентом в режиме кейса 7.
    private val client = OkHttpClient.Builder()
        .retryOnConnectionFailure(!enabledRetries)
        .followRedirects(!enabledRetries)
        .followSslRedirects(!enabledRetries)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(
        properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1)
    )
    private val pacedLimiter = if (enabledRetries) PacedLimiter(ACC7_SPACING_MS, rateLimiter) else null
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
        val maxAttempts = if (enabledRetries) MAX_ATTEMPTS else 1
        var lastSubmittedTxId: UUID? = null
        var lastResult: ExternalSysResponse? = null
        var failureReason = "No provider attempt completed"

        try {
            for (attempt in 1..maxAttempts) {
                val remainingBeforeSubmission = deadline - now()
                if (attempt > 1 && remainingBeforeSubmission < MIN_ATTEMPT_MS + FINAL_RESERVE_MS) {
                    skippedRetryCounter.increment()
                    logger.warn("[CASE7] RETRY_SKIPPED paymentId={} remainingMs={} reason=deadline", paymentId, remainingBeforeSubmission)
                    break
                }

                val transactionId = UUID.randomUUID()
                if (attempt == 1) {
                    recordSubmission(paymentId, transactionId, paymentStartedAt, deadline)
                    lastSubmittedTxId = transactionId
                }

                var retryDispatchedAt: Long? = null
                val attemptResult = try {
                    performAttempt(
                        paymentId = paymentId,
                        transactionId = transactionId,
                        amount = amount,
                        deadline = deadline,
                        attempt = attempt,
                        attemptsLeft = maxAttempts - attempt + 1,
                        prepareRetry = {
                            // Подготовленная запись остаётся с success=false, если ретрай пропущен.
                            recordSubmission(paymentId, transactionId, paymentStartedAt, deadline, success = false)
                        },
                        onRetryDispatch = {
                            retryDispatchedAt = now()
                            lastSubmittedTxId = transactionId
                            lastResult = null
                        }
                    )
                } finally {
                    val dispatchedAt = retryDispatchedAt
                    if (dispatchedAt != null) {
                        try {
                            recordSubmission(paymentId, transactionId, paymentStartedAt, deadline, submittedAt = dispatchedAt)
                        } catch (e: Exception) {
                            if (e is InterruptedException) Thread.currentThread().interrupt()
                            logger.error("[CASE7] RETRY_DISPATCH_RECORD_FAILED paymentId={} transactionId={}", paymentId, transactionId, e)
                        }
                    }
                } ?: break

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
            failureReason = "Payment error: ${e.message ?: e.javaClass.simpleName}"
        }

        val result = lastResult
        val succeeded = result?.result == true
        val reason = if (result == null) failureReason else result.message
        if (!succeeded) {
            val message = reason.orEmpty()
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
            accountName, paymentId, succeeded, reason
        )
        // Одно финальное событие Processing после всех попыток.
        paymentESService.update(paymentId) {
            it.logProcessing(succeeded, now(), lastSubmittedTxId, reason = reason)
        }
    }

    private fun recordSubmission(
        paymentId: UUID,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        success: Boolean = true,
        submittedAt: Long = now()
    ) {
        val started = System.nanoTime()
        paymentESService.update(paymentId) {
            it.logSubmission(
                success,
                transactionId,
                submittedAt,
                Duration.ofMillis((submittedAt - paymentStartedAt).coerceAtLeast(0L))
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
        attemptsLeft: Int,
        prepareRetry: () -> Unit,
        onRetryDispatch: () -> Unit
    ): AttemptResult? {
        fun skip(reason: String): AttemptResult? {
            if (attempt > 1) {
                skippedRetryCounter.increment()
                logger.warn("[CASE7] RETRY_SKIPPED paymentId={} remainingMs={} reason={}", paymentId, deadline - now(), reason)
                return null
            }
            return AttemptResult(failure(paymentId, transactionId, reason), false)
        }

        val slotWaitMs = deadline - now() - minimumAttemptMs - waitReserveMs
        if (slotWaitMs < 0L || !parallelWindow.tryAcquire(slotWaitMs)) {
            return skip("Payment deadline: no concurrency slot available")
        }

        try {
            if (attempt > 1) {
                if (deadline - now() < minimumAttemptMs + waitReserveMs) {
                    return skip("Not enough time to prepare retry")
                }
                prepareRetry()
            }

            val waitBudget = deadline - now() - minimumAttemptMs - waitReserveMs
            if (waitBudget < 0L) {
                return skip("Not enough time to process payment")
            }
            if (!acquireRatePermit(waitBudget)) {
                return skip("Insufficient time for rate limiter")
            }

            val budgetStartedNs = System.nanoTime()
            val callBudgetMs = deadline - now() - finalReserveMs
            if (callBudgetMs < minimumAttemptMs) {
                return skip("Not enough time to process payment")
            }
            val callDeadlineNs = budgetStartedNs + TimeUnit.MILLISECONDS.toNanos(callBudgetMs)

            // Оставляем время только на те повторы, которые помещаются в бюджет.
            val perAttemptMs = if (enabledRetries) {
                val retryMinimum = MIN_ATTEMPT_MS + ACC7_SPACING_MS
                val retriesThatFit = minOf((attemptsLeft - 1).toLong(), (callBudgetMs - MIN_ATTEMPT_MS) / retryMinimum)
                val attemptBudget = callBudgetMs - retriesThatFit * retryMinimum
                minOf(attemptBudget, callBudgetMs - PROVIDER_MARGIN_MS, MAX_ATTEMPT_MS)
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
                    // Duration.toString() выдаёт формат ISO-8601: PT1.591S.
                    append("&").append(PROVIDER_TIMEOUT_PARAMETER).append("=").append(Duration.ofMillis(perAttemptMs).toString())
                }
            }

            val body = if (enabledRetries) oneShotEmptyBody else emptyBody
            val request = Request.Builder().url(requestUrl).post(body).build()
            val call = client.newCall(request)
            if (callDeadlineNs - System.nanoTime() <= 0L) {
                return skip("Not enough time to process payment")
            }
            call.timeout().deadlineNanoTime(callDeadlineNs)
            if (attempt > 1) {
                onRetryDispatch()
                retryCounter.increment()
            }
            outgoingCounter.increment()

            val started = System.nanoTime()
            try {
                return call.execute().use { http ->
                    val raw = http.body?.string()
                    val isCase7Timeout = enabledRetries && http.code == 408
                    val timeoutJson = if (isCase7Timeout) {
                        try {
                            timeoutResponseReader.readTree(raw)
                        } catch (e: Exception) {
                            logger.warn("[CASE7] INVALID_TIMEOUT_RESPONSE paymentId={}", paymentId, e)
                            return@use AttemptResult(failure(paymentId, transactionId, "Invalid provider response"), false)
                        }
                    } else null
                    val response = try {
                        if (timeoutJson == null) {
                            mapper.readValue(raw, ExternalSysResponse::class.java)
                        } else {
                            mapper.treeToValue(timeoutJson, ExternalSysResponse::class.java)
                        }
                    } catch (e: Exception) {
                        logger.error("[CASE7] RESPONSE_PARSE_FAILED paymentId={} status={} body={}", paymentId, http.code, raw, e)
                        return@use AttemptResult(failure(paymentId, transactionId, "Invalid provider response"), false)
                    }

                    val canRetry = timeoutJson != null &&
                            timeoutJson.path("result").isBoolean &&
                            !response.result &&
                            timeoutJson.path("message").textValue()?.trim().equals("Timeout", ignoreCase = true) &&
                            timeoutJson.path("paymentId").textValue() == paymentId.toString() &&
                            timeoutJson.path("transactionId").textValue() == transactionId.toString()

                    if (http.code == 408) {
                        if (canRetry) confirmedTimeoutCounter.increment()
                        logger.info(
                            "[CASE7] PROVIDER_408 account={} paymentId={} retryable={} remainingMs={} msg={}",
                            accountName, paymentId, canRetry, deadline - now(), response.message
                        )
                    }
                    AttemptResult(response, canRetry)
                }
            } catch (e: InterruptedIOException) {
                networkTimeoutCounter.increment()
                logger.warn(
                    "[CASE7] NETWORK_TIMEOUT_NO_RETRY account={} paymentId={} attempt={} remainingMs={} error={}",
                    accountName, paymentId, attempt, deadline - now(), e.javaClass.simpleName
                )
                return AttemptResult(failure(paymentId, transactionId, "Request timeout."), false)
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
            parallelWindow.release()
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

private class PacedLimiter(spacingMs: Long, private val rateLimiter: SlidingWindowRateLimiter) {
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
            val remainingNs = TimeUnit.MILLISECONDS.toNanos(maxWait) - (System.nanoTime() - started)
            if (remainingNs <= 0 || !rateLimiter.tickWithTimeout(TimeUnit.NANOSECONDS.toMillis(remainingNs))) return false
            nextNs = System.nanoTime() + spacingNs
            return true
        } finally {
            lock.unlock()
        }
    }
}

fun now(): Long = System.currentTimeMillis()
