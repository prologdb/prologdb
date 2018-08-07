package com.github.prologdb.util.concurrency

import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.FreeSpec
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class RegionReadWriteLockManagerTest : FreeSpec() { init {
    "multiple read locks on non-overlapping regions" {
        RegionReadWriteLockManager().use { manager ->
            val tA = testingThread {
                manager[0..10].readLock().lock()
            }
            tA.join()
            tA.propagateUncaughtExceptionIfPresent()

            var success = false
            val tB = testingThread {
                success = manager[20..30].readLock().tryLock(100, TimeUnit.MILLISECONDS)
            }
            tB.join()
            tB.propagateUncaughtExceptionIfPresent()

            success shouldBe true
        }
    }

    "read and write locks on non-overlapping regions" {
        val manager = RegionReadWriteLockManager().use { manager ->
            val tA = testingThread {
                manager[0..10].readLock().lock()
            }
            tA.join()
            tA.propagateUncaughtExceptionIfPresent()

            var success = false
            val tB = testingThread {
                success = manager[20..30].writeLock().tryLock(100, TimeUnit.MILLISECONDS)
            }
            tB.join()
            tB.propagateUncaughtExceptionIfPresent()

            success shouldBe true
        }
    }

    "read and write lock on overlapping regions" {
        RegionReadWriteLockManager().use { manager ->
            val tAStep1Completed = CompletableFuture<Unit>()
            val tBStep2Completed = CompletableFuture<Unit>()
            val tAStep3Completed = CompletableFuture<Unit>()

            val tA = testingThread {
                manager[0..10].readLock().lock()
                tAStep1Completed.complete(Unit)

                tBStep2Completed.join()
                manager[0..10].readLock().unlock()
                tAStep3Completed.complete(Unit)
            }

            val tB = testingThread {
                tAStep1Completed.join()
                var success = manager[5..15].writeLock().tryLock(20, TimeUnit.MILLISECONDS)
                success shouldBe false
                tBStep2Completed.complete(Unit)

                tAStep3Completed.join()
                success = manager[5..15].writeLock().tryLock(20, TimeUnit.MILLISECONDS)
                success shouldBe true
            }

            tA.join()
            tA.propagateUncaughtExceptionIfPresent()
            tB.join()
            tB.propagateUncaughtExceptionIfPresent()
        }
    }

    "write locks on overlapping regions" {
        RegionReadWriteLockManager().use { manager ->
            val tAStep1Completed = CompletableFuture<Unit>()
            val tBStep2Completed = CompletableFuture<Unit>()
            val tAStep3Completed = CompletableFuture<Unit>()

            val tA = testingThread {
                manager[0..10].writeLock().lock()
                tAStep1Completed.complete(Unit)

                tBStep2Completed.join()
                manager[0..10].writeLock().unlock()
                tAStep3Completed.complete(Unit)
            }

            val tB = testingThread {
                tAStep1Completed.join()
                var success = manager[5..15].writeLock().tryLock(20, TimeUnit.MILLISECONDS)
                success shouldBe false
                tBStep2Completed.complete(Unit)

                tAStep3Completed.join()
                success = manager[5..15].writeLock().tryLock(20, TimeUnit.MILLISECONDS)
                success shouldBe true
            }

            tA.join()
            tA.propagateUncaughtExceptionIfPresent()
            tB.join()
            tB.propagateUncaughtExceptionIfPresent()
        }
    }

    "same thread can acquire overlapping write locks" {
        RegionReadWriteLockManager().use { manager ->
            manager[0 .. 10].writeLock().tryLock() shouldBe true
            manager[5 .. 15].writeLock().tryLock() shouldBe true
        }
    }

    "calling lock twice and unlock once on the same region frees the region" {
        RegionReadWriteLockManager().use { manager ->
            manager[0 .. 10].writeLock().tryLock() shouldBe true
            manager[0 .. 10].writeLock().tryLock() shouldBe true

            manager[0 .. 10].writeLock().unlock()

            var success = false
            val tA = testingThread {
                success = manager[5..15].writeLock().tryLock()
            }
            tA.join()
            tA.propagateUncaughtExceptionIfPresent()

            success shouldBe true
        }
    }

    "releasing a write lock on the same region as a read lock does not release the read lock" {
        RegionReadWriteLockManager().use { manager ->
            manager[0 .. 10].readLock().lock()
            manager[0 .. 10].writeLock().lock()

            manager[0 .. 10].writeLock().unlock()

            var success = false
            val tA = testingThread {
                success = manager[5..10].writeLock().tryLock()
            }
            tA.join()
            tA.propagateUncaughtExceptionIfPresent()
            success shouldBe false
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