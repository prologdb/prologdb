package com.github.prologdb.util.concurrency

import java.util.concurrent.ConcurrentHashMap

/**
 * Very much like [ThreadLocal] except that the [clear] method has been added.
 */
class ClearableThreadLocal<T>(
    /** Calculates and returns a new default value */
    val defaultValue: () -> T,

    /** When [clear] is called, this is invoked for all cleared values */
    private val teardown: (T) -> Unit = {}
) {
    fun set(o: T) {
        val thread = Thread.currentThread()
        @Suppress("uncheckedCast") val map = perThread.getOrPut(thread) { HashMap<ClearableThreadLocal<*>, Any>() } as MutableMap<ClearableThreadLocal<T>, T>

        map[this] = o
    }

    fun get(): T {
        val thread = Thread.currentThread()
        @Suppress("uncheckedCast") val map = perThread.getOrPut(thread) { HashMap<ClearableThreadLocal<*>, Any>() } as MutableMap<ClearableThreadLocal<T>, T>

        val o = map[this]
        if (o != null) {
            return o
        }

        val no = defaultValue()
        map[this] = no
        return no
    }

    companion object {
        /**
         * For all threads, clears the data associated with the given [threadLocal]
         */
        fun <T> clearForAllThreads(threadLocal: ClearableThreadLocal<T>) {
            perThread.values.forEach {
                val value = it.remove(threadLocal)
                if (value != null) threadLocal.teardown(value as T)
            }
        }

        @JvmStatic
        private val perThread: MutableMap<Thread, MutableMap<ClearableThreadLocal<*>, *>> = ConcurrentHashMap()
    }
}