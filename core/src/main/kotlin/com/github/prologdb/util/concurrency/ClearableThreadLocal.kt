package com.github.prologdb.util.concurrency

import java.util.concurrent.ConcurrentHashMap

/**
 * Very much like [ThreadLocal] except that the [clear] method has been added.
 */
class ClearableThreadLocal<T>(val defaultValue: () -> T) {
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
        fun clearForAllThreads(threadLocal: ClearableThreadLocal<*>) {
            perThread.values.forEach { it.remove(threadLocal) }
        }

        @JvmStatic
        private val perThread: MutableMap<Thread, MutableMap<ClearableThreadLocal<*>, *>> = ConcurrentHashMap()
    }
}