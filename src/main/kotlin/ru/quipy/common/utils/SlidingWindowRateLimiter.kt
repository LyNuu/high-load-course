
package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {

    private val rateLimiterScope = CoroutineScope(
        Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    )

    private val sum = AtomicLong(0)
    private val queue = PriorityBlockingQueue<Measure>(10_000)

    override fun tick(): Boolean {
        while (true) {
            val curSum = sum.get()

            if (curSum >= rate) {
                return false
            }

            if (sum.compareAndSet(curSum, curSum + 1)) {
                queue.add(
                    Measure(1, System.currentTimeMillis())
                )
                return true
            }
        }
    }

    // Старое поведение: ждать без ограничения.
    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

    // Новое поведение: ждать не дольше timeoutMs.
    fun tickWithTimeout(timeoutMs: Long): Boolean {
        val timeoutNanos = TimeUnit.MILLISECONDS.toNanos(
            timeoutMs.coerceAtLeast(0)
        )

        val startedAt = System.nanoTime()

        while (true) {
            if (tick()) {
                return true
            }

            val elapsedNanos = System.nanoTime() - startedAt
            val remainingNanos = timeoutNanos - elapsedNanos

            if (remainingNanos <= 0L) {
                return false
            }

            try {
                TimeUnit.NANOSECONDS.sleep(
                    minOf(remainingNanos, 10_000_000L)
                )
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return false
            }
        }
    }

    data class Measure(
        val value: Long,
        val timestamp: Long
    ) : Comparable<Measure> {

        override fun compareTo(other: Measure): Int {
            return timestamp.compareTo(other.timestamp)
        }
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val head = queue.peek()
            val winStart =
                System.currentTimeMillis() - window.toMillis()

            if (head == null) {
                delay(1L)
                continue
            }

            if (head.timestamp > winStart) {
                delay(head.timestamp - winStart)
                continue
            }

            sum.addAndGet(-1)
            queue.take()
        }
    }.invokeOnCompletion { th ->
        if (th != null) {
            logger.error(
                "Rate limiter release job completed",
                th
            )
        }
    }

    companion object {
        private val logger: Logger =
            LoggerFactory.getLogger(
                SlidingWindowRateLimiter::class.java
            )
    }
}
