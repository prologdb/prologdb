package com.github.prologdb.io.util

import java.util.concurrent.LinkedBlockingQueue

/**
 * A pool of objects for use by multiple threads.
 * @param <T>
 */
class Pool<T>(
    val minSize: Int,
    private val initializer: () -> T,

    /** Is called when objects are returned to the pool. */
    private val sanitizer: (T) -> Any? =  { _ -> }
) {
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
        val o = get()
        val r = action(o)
        free(o)

        return r
    }

    fun get(): T {
        return storage.poll() ?: initializer()
    }

    fun free(o: T) {
        sanitizer(o)
        storage.offer(o)
    }

    /**
     * Releases all the objects in the pool.
     */
    fun clear() {
        storage.clear()
    }
}
