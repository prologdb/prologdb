package com.github.prologdb.util.concurrency

import java.util.concurrent.LinkedBlockingQueue

/**
 * A pool of objects for use by multiple threads.
 * @param <T>
 */
class Pool<T>(val minSize: Int, private val initializer: () -> T) {
    private val storage = LinkedBlockingQueue<T>(minSize)

    init {
        if (minSize < 0) throw IllegalArgumentException("The minimum pool size must be 0 or greater.")
        for (i in 0..minSize) {
            storage.offer(initializer())
        }
    }

    /**
     * Obtains an object from the pool and runs the given action with it; forwards
     * the return value. After the action has executed successfully, the object is
     * returned to the pool.
     *
     * Exceptions are forwarded. If an exceptions is thrown by the action,
     * the object is not returned to the pool.
     */
    fun <R> using(action: (T) -> R): R {
        val o = storage.poll() ?: initializer()
        val r = action(o)
        storage.offer(o)

        return r
    }
}
