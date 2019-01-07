package com.github.prologdb.net.async

import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Similar to a non-blocking queue:
 *
 * An (theoretically) unlimited number of items can be queued but only
 * one is consumed at a time. When the [queue] method is called and currently
 * no item is being consumed, consumption will start immediately. Otherwise
 * the item is queued and consumed later.
 *
 * **This class is thread-safe!**
 */
class AsyncPipe<Item, ConsumptionResult>(private val consumer: (Item, CompletableFuture<ConsumptionResult>) -> Any?) {
    private val itemQueue = ArrayBlockingQueue<Pair<Item, CompletableFuture<ConsumptionResult>>>(20)

    /**
     * To be synchronized on when modifying [itemQueue]. This is necessary to prevent a race condition when
     * a new item is queued in the same moment that the batch finishes ([queue] is invoked, checks that current
     * batch is running; current batch detects the queue is empty and quits; item gets appended to queue and is
     * not given to the consumer).
     * This fact also allowes [itemQueue] not to be thread safe.
     */
    private val queueMutex = Any()

    /** True while items are being consumed */
    private val currentlyConsuming = AtomicBoolean(false)

    fun queue(item: Item): Future<ConsumptionResult> {
        if (closed) throw PipeClosedException()

        val cFuture = CompletableFuture<ConsumptionResult>()

        synchronized(queueMutex) {
            itemQueue.put(Pair(item, cFuture))
        }

        // launch the sender again
        if (currentlyConsuming.compareAndSet(false, true)) {
            consume {
                if (!currentlyConsuming.compareAndSet(true, false)) {
                    throw ConcurrentModificationException()
                }
            }
        }

        return cFuture
    }

    /**
     * Consumes the given item and succeeds/errors the given `callback` once done.
     * If there are items in the [queue]
     */
    private fun consume(onQueueDepleted: () -> Any?) {
        val nextItem = synchronized(queueMutex) { itemQueue.poll() }

        if (nextItem == null) {
            onQueueDepleted()
            return
        }

        val (item, callback) = nextItem

        callback.whenComplete { r, e ->
            consume(onQueueDepleted)
        }
        consumer(item, callback)
    }

    @Volatile var closed = false
        private set
    private val closingMutex = Any()

    /**
     * Blocks until all queued items have been given to the consumer.
     */
    fun close() {
        if (closed) return
        synchronized(closingMutex) {
            if (closed) return
            closed = true
        }

        while (itemQueue.isNotEmpty()) {
            try {
                Thread.sleep(100)
            }
            catch (ex: InterruptedException) {}
        }
    }
}

class PipeClosedException : IllegalStateException("Pipe closed")