package com.github.prologdb.util.concurrency.locks

import com.github.prologdb.async.Principal
import java.util.concurrent.Future

/**
 * Similar to [java.util.concurrent.locks.Lock] but with Async features
 */
interface AsyncLock {
    /**
     * Acquires the lock.
     * @return A future that completes once the lock is acquired. Completes exceptionally if the lock cannot
     *         be acquired.
     */
    fun acquireFor(principal: Principal): Future<Unit>

    /**
     * Releases the lock. Returns immediately.
     */
    fun releaseFor(principal: Principal)
}