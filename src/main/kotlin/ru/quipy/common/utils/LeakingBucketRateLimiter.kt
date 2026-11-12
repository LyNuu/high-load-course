
package ru.quipy.common.utils

import java.time.Duration

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    private val bucketSize: Int,
) : RateLimiter {

    init {
        require(rate > 0)
        require(bucketSize > 0)
    }

    private val intervalNanos = window.toNanos() / rate
    private var nextAllowedTime = System.nanoTime()

    override fun tick(): Boolean = tick(bucketSize)

    @Synchronized
    fun tick(burstSize: Int): Boolean {
        require(burstSize > 0)

        val currentTime = System.nanoTime()
        val burstNanos = (burstSize - 1L) * intervalNanos

        if (currentTime < nextAllowedTime - burstNanos) {
            return false
        }

        nextAllowedTime =
            maxOf(currentTime, nextAllowedTime) + intervalNanos

        return true
    }
}
