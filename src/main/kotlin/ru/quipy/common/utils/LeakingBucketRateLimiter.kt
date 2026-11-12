package ru.quipy.common.utils

import java.time.Duration

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    bucketSize: Int,
) : RateLimiter {

    init {
        require(rate > 0)
        require(bucketSize > 0)
    }

    private val intervalNanos = window.toNanos() / rate

    private val burstNanos =
        (bucketSize - 1L) * intervalNanos

    private var nextAllowedTime = System.nanoTime()

    @Synchronized
    override fun tick(): Boolean {
        val currentTime = System.nanoTime()

        if (currentTime < nextAllowedTime - burstNanos) {
            return false
        }

        nextAllowedTime =
            maxOf(currentTime, nextAllowedTime) + intervalNanos

        return true
    }
}
