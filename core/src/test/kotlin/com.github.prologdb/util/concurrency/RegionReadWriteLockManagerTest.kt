package com.github.prologdb.util.concurrency

import com.github.prologdb.util.concurrency.locks.RegionReadWriteLockManager
import io.kotlintest.matchers.shouldThrow
import io.kotlintest.specs.FreeSpec
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class RegionReadWriteLockManagerTest : FreeSpec() { init {
    "multiple read locks on non-overlapping regions" {
        RegionReadWriteLockManager().use { manager ->
            manager[0 ..10].readLock.acquireFor("A").get(50, TimeUnit.MILLISECONDS)
            manager[20..30].readLock.acquireFor("B").get(50, TimeUnit.MILLISECONDS)
        }
    }

    "read and write locks on non-overlapping regions" {
        RegionReadWriteLockManager().use { manager ->
            manager[0 ..10].readLock.acquireFor("A").get(50, TimeUnit.MILLISECONDS)
            manager[20..30].writeLock.acquireFor("B").get(50, TimeUnit.MILLISECONDS)
        }
    }

    "read and write lock on overlapping regions" {
        RegionReadWriteLockManager().use { manager ->
            manager[0 .. 10].readLock.acquireFor("A").get(50, TimeUnit.MILLISECONDS)
            val writeLockAcquired = manager[5 .. 15].writeLock.acquireFor("B")
            shouldThrow<TimeoutException> {
                writeLockAcquired.get(50, TimeUnit.MILLISECONDS)
            }

            manager[0 .. 10].readLock.releaseFor("A")
            writeLockAcquired.get(50, TimeUnit.MILLISECONDS)
        }
    }

    "write locks on overlapping regions" {
        RegionReadWriteLockManager().use { manager ->
            manager[0 .. 10].writeLock.acquireFor("A").get(50, TimeUnit.MILLISECONDS)
            val writeLockAcquired = manager[5 .. 15].writeLock.acquireFor("B")
            shouldThrow<TimeoutException> {
                writeLockAcquired.get(50, TimeUnit.MILLISECONDS)
            }

            manager[0 .. 10].writeLock.releaseFor("A")
            writeLockAcquired.get(50, TimeUnit.MILLISECONDS)
        }
    }

    "same thread can acquire overlapping write locks" {
        RegionReadWriteLockManager().use { manager ->
            manager[0 .. 10].writeLock.acquireFor(Thread.currentThread()).get(20, TimeUnit.MILLISECONDS)
            manager[5 .. 15].writeLock.acquireFor(Thread.currentThread()).get(20, TimeUnit.MILLISECONDS)
        }
    }

    "calling lock twice and unlock once on the same region frees the region" {
        RegionReadWriteLockManager().use { manager ->
            manager[0 .. 10].writeLock.acquireFor(Thread.currentThread()).get()
            manager[0 .. 10].writeLock.acquireFor(Thread.currentThread()).get()

            manager[0 .. 10].writeLock.releaseFor(Thread.currentThread())

            manager[5 .. 15].writeLock.acquireFor("other principal").get(20, TimeUnit.MILLISECONDS)
        }
    }

    "releasing a write lock on the same region as a read lock does not release the read lock" {
        RegionReadWriteLockManager().use { manager ->
            manager[0 .. 10].readLock.acquireFor("principal")
            manager[0 .. 10].writeLock.acquireFor("principal")

            manager[0 .. 10].writeLock.releaseFor("principal")

            shouldThrow<TimeoutException> {
                manager[5..10].writeLock.acquireFor("other principal").get(200, TimeUnit.MILLISECONDS)
            }
        }
    }
}}

private fun testingThread(code: () -> Unit): TestingThread {
    val thread = TestingThread(code)
    thread.start()
    return thread
}

private class TestingThread(code: () -> Unit) : Thread(code) {
    private var uncaughtEx: Throwable? = null
    init {
        setUncaughtExceptionHandler { t, e ->
            uncaughtEx = e
        }
    }

    fun propagateUncaughtExceptionIfPresent() {
        if (uncaughtEx != null) {
            throw uncaughtEx!!
        }
    }
}