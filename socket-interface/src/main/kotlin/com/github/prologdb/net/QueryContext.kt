package com.github.prologdb.net

import com.github.prologdb.async.LazySequence
import com.github.prologdb.net.session.QueryRelatedError
import com.github.prologdb.net.util.prettyPrint
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.unification.Unification
import java.util.*
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.min

/**
 * Context of a query. Has all necessary methods to operate
 * the query in any way possible.
 *
 * Query contexts are **NOT THREAD-SAFE!**. Use [ifAvailable]
 * for interaction.
 */
internal class QueryContext(
    private val queryId: Int,
    private val solutions: LazySequence<Unification>,
    private var precalculationLimit: Long
) {
    private val lock: Lock = ReentrantLock()

    private val precalculations: Queue<Unification> = ArrayDeque(min(precalculationLimit, 1024L).toInt())

    private val hasPrecalculationsOutstanding: Boolean
        get() = precalculations.size.toLong() < precalculationLimit

    private val actionInterface = ActionInterface()

    var closed = false
        private set

    /**
     * If currently no other thread is working on this context and
     * the context is not closed, locks the context and executes the
     * action. The return value and exceptions are forwarded from the
     * action. The lock is released before this method returns in any case.
     *
     * @return first: whether the action was executed, second: the
     * forwarded return value
     */
    fun <T> ifAvailable(action: (ActionInterface) -> T): Pair<Boolean, T?> {
        if (lock.tryLock()) {
            try {
                if (!closed) {
                    return Pair(true, action(actionInterface))
                }
            } finally {
                lock.unlock()
            }
        }

        return Pair(false, null)
    }

    /**
     * Locks the context, possibly waiting interruptibly for the lock
     * to become available. Once acquired runs the given action. Return
     * value and exceptions are forwarded. The lock is released in any
     * case.
     */
    fun <T> doWith(action: (ActionInterface) -> T): T {
        if (closed) throw IllegalStateException("Query context already closed.")

        return lock.withLock {
            if (closed) throw IllegalStateException("Query context already closed!")
            action(actionInterface)
        }
    }

    /**
     * Interface to interact with the context. Separated from the other code
     * to 100% enforce thread-safety.
     */
    inner class ActionInterface internal constructor() {
        /**
         * If needed, does one precalculation.
         * @return whether there are more outstanding.
         */
        fun doOnePrecalculation(): Boolean {
            if (!hasPrecalculationsOutstanding) return false

            val solution = solutions.tryAdvance()
            // this wont block or trip because ArrayDeque
            assert(precalculations.offer(solution), { "Precalculations queue out of capacity. Somethings wrong here!" })

            return hasPrecalculationsOutstanding
        }

        /**
         * [MutableCollection.add]s up to `limit` precalculated solutions to `target`.
         *
         * @return the actual number of solutions added.
         */
        fun drainPrecalculationsTo(target: MutableCollection<in Unification>, limit: Int = Integer.MAX_VALUE): Int {
            val nToDrain = min(limit, precalculations.size)
            for (i in 1..nToDrain) {
                target.add(precalculations.poll() ?: throw RuntimeException("Queue must not be empty, race condition?"))
            }

            return nToDrain
        }

        /**
         * Calculates up to `limit` solutions (up to [Integer.MAX_VALUE]) and
         * adds them to the given collection.
         * **Important:** this method does not respect the precalculations. Call [drainPrecalculationsTo] first!
         *
         * @param limit Maximum number of solutions to calculate. Must be less than [Integer.MAX_VALUE]
         * @return The number of solutions actually added. Is less than the requested
         *         limit if the solutions are depleted.
         */
        fun calculateSolutionsInto(target: MutableCollection<in Unification>, limit: Int): Int {
            if (limit >= Integer.MAX_VALUE) {
                throw IllegalArgumentException("Limit must be less than ${Integer.MAX_VALUE}")
            }

            var added = 0
            while (added < limit) {
                val solution = try {
                    solutions.tryAdvance()
                }
                catch (ex: PrologRuntimeException) {
                    throw QueryRelatedException(
                        QueryRelatedError(
                            queryId,
                            QueryRelatedError.Kind.ERROR_GENERIC,
                            ex.message,
                            mapOf("prologStackTrace" to ex.prettyPrint())
                        )
                    )
                }
                catch (ex: Throwable) {
                    throw NetworkProtocolException(
                        "Internal server error",
                        ex
                    )
                }

                if (solution == null) break

                target.add(solution)
                added++
            }

            return added
        }

        /**
         * Closes the context, releasing any open resources.
         */
        fun close() {
            solutions.close()
            precalculations.clear()
        }

        var precalculationLimit: Long
            get() = this@QueryContext.precalculationLimit
            set(value) {
                if (value < 0) throw IllegalArgumentException("Cannot set negative precalculation limiu")
                this@QueryContext.precalculationLimit = value
            }
    }
}

