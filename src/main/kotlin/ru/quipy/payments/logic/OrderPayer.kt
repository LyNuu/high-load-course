
package ru.quipy.payments.logic

import io.micrometer.core.instrument.Metrics
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger =
            LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService:
            EventSourcingService<
                    UUID,
                    PaymentAggregate,
                    PaymentAggregateState
                    >

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        256,
        256,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(64),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    private val incomingCounter =
        Metrics.counter("shop.payments.incoming")

    fun processPayment(
        orderId: UUID,
        amount: Int,
        paymentId: UUID,
        deadline: Long
    ): Long {
        incomingCounter.increment()

        val createdAt = System.currentTimeMillis()

        try {
            paymentExecutor.submit {
                val taskStartedAt = System.currentTimeMillis()
                val queueWaitMs = taskStartedAt - createdAt

                try {
                    // Измеряем время создания платежа в Event Sourcing.
                    val dbStart = System.nanoTime()

                    val createdEvent = paymentESService.create {
                        it.create(
                            paymentId,
                            orderId,
                            amount
                        )
                    }

                    val dbCreateMs =
                        (System.nanoTime() - dbStart) / 1_000_000

                    val remainingMs =
                        deadline - System.currentTimeMillis()

                    // Логируем медленные операции.
                    if (queueWaitMs > 100L || dbCreateMs > 200L) {
                        logger.warn(
                            "[PAYMENT-DIAG] [ORDER_TIMING] " +
                                    "paymentId={} queueWaitMs={} " +
                                    "dbCreateMs={} remainingMs={} " +
                                    "active={} queued={}",
                            paymentId,
                            queueWaitMs,
                            dbCreateMs,
                            remainingMs,
                            paymentExecutor.activeCount,
                            paymentExecutor.queue.size
                        )
                    }

                    // Диагностика просроченных платежей.
                    if (remainingMs <= 0L) {
                        logger.warn(
                            "[PAYMENT-DIAG] [DEADLINE_ALREADY_EXPIRED] " +
                                    "paymentId={} queueWaitMs={} dbCreateMs={}",
                            paymentId,
                            queueWaitMs,
                            dbCreateMs
                        )
                    }

                    logger.trace(
                        "Payment ${createdEvent.paymentId} " +
                                "for order $orderId created."
                    )

                    // Не меняем бизнес-логику:
                    // передаём платёж дальше даже при истёкшем deadline.
                    // Это сохраняет обработку событий Event Sourcing.
                    paymentService.submitPaymentRequest(
                        paymentId,
                        amount,
                        createdAt,
                        deadline
                    )

                } catch (e: Exception) {
                    logger.error(
                        "[PAYMENT-DIAG] [PAYMENT_TASK_FAILED] " +
                                "paymentId={} orderId={} queueWaitMs={}",
                        paymentId,
                        orderId,
                        queueWaitMs,
                        e
                    )
                }
            }

        } catch (e: RejectedExecutionException) {
            logger.warn(
                "[PAYMENT-DIAG] [EXECUTOR_REJECTED] " +
                        "paymentId={} active={} queued={}",
                paymentId,
                paymentExecutor.activeCount,
                paymentExecutor.queue.size
            )

            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                "Payment buffer is full"
            )
        }

        return createdAt
    }
}
