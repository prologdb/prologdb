package com.github.prologdb.util.concurrency

import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue

/**
 * A queue that allows multiple consumers to consume all items from the
 * queue individually: every consumer will consume every item in the queue.
 */
class BroadcastConcurrentQueue<T> {

    /**
     * This is a hashmap with synchronized() because the use cases this class is used for
     * in this project have very few reads&writes to the queues. To suit this for more
     * throughput a [ConcurrentHashMap] can be used.
     */
    private val consumerQueues: MutableMap<Long, Queue<T>> = HashMap()

    /**
     * Registers a new consumer with the reference `consumerID` if not already
     * registered.
     * @return The queue for the registered consumer.If the consumer was not registered
     * prior to the call, does **not** contain items placed on the queue prior to registering.
     */
    fun registerConsumer(consumerID: Long): Queue<T> {
        synchronized(consumerQueues) {
            return consumerQueues.computeIfAbsent(consumerID) { LinkedBlockingQueue<T>() }
        }
    }

    /**
     * Unregisters the consumer with the given ID from this queue.
     */
    fun unregisterConsumer(consumerID: Long) {
        synchronized(consumerQueues) {
            consumerQueues.remove(consumerID)?.clear()
        }
    }

    /**
     * Adds the given item as the new tail of this queue.
     */
    fun add(item: T) {
        synchronized(consumerQueues) {
            consumerQueues.values.forEach {
                it.add(item)
            }
        }
    }
}