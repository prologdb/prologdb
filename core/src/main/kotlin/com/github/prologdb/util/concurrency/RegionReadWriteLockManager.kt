package com.github.prologdb.util.concurrency

import java.io.Closeable
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReadWriteLock

/**
 * For some contiguous thing where single units can be represented by offsets of type [Long]
 * (e.g. a file or a contiguous block of memory), offers the following mechanism:
 *
 * Read locks can be obtained for any region of the resource; read-locked regions may overlap.
 * Acquiring a write lock requires that no other read or write lock overlaps with the region
 * for the requested write lock.
 * Locks are granted in the chronological order they were requested.
 *
 * For this class to be of any use, the user must make sure that there is only one instance of this
 * class for a given resource.
 *
 * This class is thread-safe. The actual size of the underlying resource is not of concern for this
 * class.
 *
 * This class is named RegionReadWriteLockManager for lack of a better name.
 */
internal class RegionReadWriteLockManager(val name: String? = null) : AutoCloseable, Closeable {
    /**
     * For every invocation of [Lock.lock], a request is put onto this queue for
     * the [lockGrantThread] to pick it up and grant the lock
     *
     * When the manager is closed it is the responsibility of the manager (as opposed to the [lockGrantThread])
     * to [CompletableFuture.completeExceptionally] all of these.
     */
    private val lockRequestQueue: LinkedBlockingDeque<LockRequest> = LinkedBlockingDeque()

    /**
     * For every invocation of [Lock.unlock], the region to be unlocked is put onto this
     * queue for the [lockGrantThread] to pick it up and free the lock.
     *
     * When the manager is closed it is the responsibility of the manager (as opposed to the [lockGrantThread])
     * to [CompletableFuture.complete] all of these.
     */
    private val unlockRequestQueue: BlockingQueue<UnlockRequest> = LinkedBlockingDeque()

    /** Caches return values of [get] per thread */
    private val readWriteLockCache: MutableMap<LongRange, ReadWriteLock> = WeakHashMap()

    /** Set to non-null when this manager is closed. Locks cannot be acquired after that */
    private var closeMode: CloseMode? = null

    /**
     * Must be completed by the [granterThread] when it shuts down; if it quits with an exception,
     * this future must be completed exceptionally.
     *
     * Used by [close] in [CloseMode.WAITING] to determine when closing is completed.
     */
    private val granterThreadShutdown = CompletableFuture<Unit>()

    /**
     * Grants the locks; doing this in a background thread assures order of grants.
     * Must be [Thread.interrupt]ed when
     * * new free requests are available
     * * the manager is closed (to quit loops)
     */
    private val lockGrantThread: Thread

    init {
        lockGrantThread = Thread(FairGranterRunnable(), "RegionReadWriteLockManager(${name ?: "?"})")
        lockGrantThread.start()
    }

    /**
     * @throws ClosedException If this manager has been closed, see [close]
     */
    operator fun get(region: LongRange): ReadWriteLock {
        if (region.first > region.last) throw IllegalArgumentException("The given region must be ascending")
        if (region.isEmpty()) throw IllegalArgumentException("Given region is empty, cannot be locked.")

        if (closeMode != null) throw ClosedException("This manager has been closed.")

        val cached = readWriteLockCache[region]
        if (cached != null) return cached

        synchronized(readWriteLockCache) {
            val lock = ReadWriteLockHandle(region)
            readWriteLockCache[region] = lock
            return lock
        }
    }

    /**
     * See `get(LongRange)`
     */
    operator fun get(region: IntRange): ReadWriteLock = get(LongRange(region.start.toLong(), region.last.toLong()))

    /**
     * Closes this manager, with the following effects:
     * * subsequent calls to [get] will throw a [ClosedException]
     * * all locks will be released
     * * calling [Lock.tryLock] on any of the locks obtained from this manager will return false
     * * calling [Lock.lock] or [Lock.lockInterruptibly] on any of the locks obtained from this manager will throw
     *   a [ClosedException]
     */
    fun close(mode: CloseMode) {
        // prevent new queueing
        closeMode = mode
        lockGrantThread.interrupt()

        when (mode) {
            CloseMode.IMMEDIATELY -> {
                val closedException = ClosedException("The manager has been closed while you were waiting for the lock.")
                lockRequestQueue.forEach { it.onGrantComplete.completeExceptionally(closedException) }
                unlockRequestQueue.clear()
            }
            CloseMode.WAITING -> {
                // wait for all outstanding locks to be granted.
                while (lockRequestQueue.isNotEmpty()) {
                    lockRequestQueue.peekLast()?.onGrantComplete?.join()
                }

                // now wait for these to be freed
                granterThreadShutdown.join()

                // just to be sure
                lockRequestQueue.clear()
                unlockRequestQueue.clear()
            }
        }
    }

    /**
     * Delegates to `close(CloseMode.IMMEDIATELY)`
     */
    override fun close() = close(CloseMode.IMMEDIATELY)

    /**
     * Grants locks in a fair way - first requested, first to acquire
     */
    private inner class FairGranterRunnable : Runnable {

        /**
         * All active read locks
         *
         * Sorted by the starting position of the region. This makes finding overlaps faster.
         */
        private val activeReadLocks = TreeSet<Pair<Thread, LongRange>>(compareBy { it.second.first })

        /**
         * All active write locks
         *
         * Sorted by the starting position of the region. This makes finding overlaps faster.
         */
        private val activeWriteLocks = TreeSet<Pair<Thread, LongRange>>(compareBy { it.second.first })

        override fun run() {
            try {
                doRun()
                granterThreadShutdown.complete(Unit)
            }
            catch (ex: Throwable) {
                granterThreadShutdown.completeExceptionally(ex)
                throw ex
            }
        }

        private fun doRun() {
            /** Re-used for [BlockingQueue.drainTo] with [unlockRequestQueue] */
            val toBeFreedBucket: MutableList<UnlockRequest> = ArrayList(10)

            while (closeMode != CloseMode.IMMEDIATELY) {
                // first try to free locks; that makes unlocks fast and acquires more likely
                toBeFreedBucket.clear()
                unlockRequestQueue.drainTo(toBeFreedBucket)
                toBeFreedBucket.forEach(this::doUnlock)

                // try to acquire the next lock in line; if not possible, wait for more unlocks
                val request: LockRequest? = try {
                    if (closeMode == null) lockRequestQueue.take() else lockRequestQueue.poll()
                } catch (ex: InterruptedException) {
                    // more frees may be available or the manager might have been closed
                    continue
                }

                if (request == null) {
                    if (closeMode != null) {
                        // all done
                        break
                    }
                }
                else
                {
                    val locked = tryLock(request)
                    if (locked) {
                        request.onGrantComplete.complete(Unit)
                    }
                    else
                    {
                        // cannot acquire; wait for more frees to become available
                        val newUnlockRequest = try {
                            unlockRequestQueue.take()
                        } catch (ex: InterruptedException) {
                            // more frees may be available or the manager might have been closed
                            continue
                        }

                        doUnlock(newUnlockRequest)
                    }
                }
            }

            // closed
            activeReadLocks.clear()
            activeWriteLocks.clear()
        }

        /**
         * Assures the region associated with the given request is not locked.
         */
        private fun doUnlock(request: UnlockRequest) {
            when (request.mode) {
                LockMode.READ -> activeReadLocks.removeIf { it.first == request.thread && it.second == request.region }
                LockMode.READ_WRITE -> activeWriteLocks.removeIf { it.first == request.thread && it.second == request.region }
            }
        }

        /**
         * Tries to lock the region of the given request in the given mode.
         * @return Whether the region was available in the given mode and thus the locking succeeded
         */
        private fun tryLock(request: LockRequest): Boolean {
            val collidingWriteLocksExist = activeWriteLocks
                .filter { it.first != request.thread }
                .any { it.second overlapsWith request.region }

            if (collidingWriteLocksExist) {
                return false
            }

            when (request.mode) {
                LockMode.READ_WRITE -> {
                    val collidingReadLocksExist = activeReadLocks
                        .filter { it.first != request.thread }
                        .any { it.second overlapsWith request.region }

                    if (collidingReadLocksExist) {
                        return false
                    }

                    activeWriteLocks.add(Pair(request.thread, request.region))
                    return true
                }
                LockMode.READ -> {
                    // no more cheitgcks necessary, read locks may overlap
                    activeReadLocks.add(Pair(request.thread, request.region))
                    return true
                }
            }
        }
    }

    private inner class ReadWriteLockHandle(region: LongRange) : ReadWriteLock {
        private val readHandle = LockHandle(region, LockMode.READ)
        private val writeHandle = LockHandle(region, LockMode.READ_WRITE)

        override fun writeLock(): Lock = writeHandle
        override fun readLock(): Lock = readHandle
    }

    private inner class LockHandle(val region: LongRange, val mode: LockMode) : Lock {
        override fun lock() {
            if (closeMode != null) throw ClosedException("The manager has already been closed - cannot acquire lock")

            lock(false)
        }

        @Synchronized override fun tryLock(): Boolean {
            if (closeMode != null) return false

            // granting is done by another thread; even though the region might be free currently,
            // we cannot just start messing about with the granter threads data here. Checking whether
            // the lock is free might also take some time and when we then start to acquire the lock,
            // it might be blocked. This is a trade-off.
            return tryLock(5, TimeUnit.MILLISECONDS)
        }

        @Synchronized override fun tryLock(time: Long, unit: TimeUnit?): Boolean {
            if (closeMode != null) return false

            if (Thread.interrupted()) throw InterruptedException()

            return lock(true, time, unit!!)
        }

        override fun unlock() {
            if (closeMode != null) throw ClosedException("The manager has already been closed - cannot acquire lock")

            unlockRequestQueue.put(UnlockRequest(region, mode, Thread.currentThread()))
            lockGrantThread.interrupt()
        }

        @Synchronized override fun lockInterruptibly() {
            if (closeMode != null) throw ClosedException("The manager has already been closed - cannot acquire lock")

            if (Thread.interrupted()) throw InterruptedException()

            lock(true)
        }

        override fun newCondition(): Condition {
            throw NotImplementedError("Conditions are not implemented on RegionReadWriteLockManager Locks, sorry.")
        }

        /**
         * Acquires the lock.
         * @param canBeInterrupted Whether [InterruptedException]s should be propagated; if false, are caught and waiting is resumed
         * @param timeoutAmount Amount of the timeout. 0 means infinite.
         * @param timeoutUnit Unit of [timeoutAmount]
         */
        private fun lock(canBeInterrupted: Boolean, timeoutAmount: Long = 0, timeoutUnit: TimeUnit = TimeUnit.MILLISECONDS): Boolean {
            val granted = CompletableFuture<Unit>()
            lockRequestQueue.put(LockRequest(region, mode, Thread.currentThread(), granted))

            // completable future join() cannot be interrupted
            try {
                if (canBeInterrupted) {
                    if (timeoutAmount == 0L) {
                        granted.get()
                    } else {
                        // throws timeout exception when the timeout is reached
                        granted.get(timeoutAmount, timeoutUnit)
                    }

                    return true
                }
                else {
                    if (timeoutAmount == 0L) {
                        // join cannot be interrupted
                        granted.join()
                        return true
                    } else {
                        val timeoutInNanos = timeoutUnit.toNanos(timeoutAmount)
                        var totalWaitedTimeNanos = 0L
                        while (true) {
                            val waitStartedAt = System.nanoTime()

                            try {
                                // throws timeout exception when the timeout is reached
                                // interrupted exception when interrupted
                                granted.get(timeoutAmount, timeoutUnit)
                                // success!
                                return true
                            }
                            catch (ex: InterruptedException) {
                                val interruptedAt = System.nanoTime()
                                totalWaitedTimeNanos += interruptedAt - waitStartedAt
                                if (totalWaitedTimeNanos >= timeoutInNanos) {
                                    // the timeout has been reached
                                    throw TimeoutException()
                                }
                            }
                        }
                    }
                }
            }
            catch (ex: CompletionException) {
                throw ex.cause ?: ex
            }
            catch (ex: TimeoutException) {
                return false
            }
        }
    }

    private data class LockRequest(
        /** The region to lock */
        val region: LongRange,
        val mode: LockMode,
        /** The thread requesting the lock */
        val thread: Thread,
        /** When the lock is granted, this future is to be completed */
        val onGrantComplete: CompletableFuture<Unit>
    )

    private data class UnlockRequest(
        /** The region to free; must match one that was requested using a [LockRequest] */
        val region: LongRange,
        val mode: LockMode,
        /** The thread releasing the lock */
        val thread: Thread
    )

    private enum class LockMode {
        READ,
        READ_WRITE
    }

    /**
     * Thrown when the manager is closed and operations are attempted (or running) that cannot be completed
     * in that situation (e.g. waiting [Lock.lock] calls).
     */
    class ClosedException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

    /**
     * The different ways this manager can be closed.
     */
    enum class CloseMode {
        /**
         * Prevents new grants from being queued and then
         * * aborts all queued grants with a [ClosedException]
         * * closes open locks
         */
        IMMEDIATELY,

        /**
         * Prevents new grants from being queued and then
         * * waits for all currently queued grants to be granted
         * * then, waits for all currently open locks to be released
         */
        WAITING
    }
}

private infix fun LongRange.overlapsWith(other: LongRange): Boolean {
    // thanks to https://stackoverflow.com/questions/3269434/whats-the-most-efficient-way-to-test-two-integer-ranges-for-overlap/3269471

    return this.first <= other.last && other.first <= this.last
}