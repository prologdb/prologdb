package com.github.prologdb.net

import com.github.prologdb.async.LazySequence
import com.github.prologdb.net.session.ConsumeQuerySolutionsCommand
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.unification.Unification
import org.slf4j.LoggerFactory
import java.util.ArrayDeque
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

private val log = LoggerFactory.getLogger("prologdb.worker")

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
    val originalQuery: Query
) {
    private val lock: Lock = ReentrantLock()

    /**
     * Consumption requests. The head of the queue remains there until it is fully
     * handled (see [currentlyConsumed]).
     */
    private val consumptionRequests = ArrayDeque<ConsumeQuerySolutionsCommand>()

    /**
     * The amount of solutions already consumed **from the head of [consumptionRequests]**.
     * Is reset to 0 when the head of [consumptionRequests] is completed and removed.
     */
    private var currentlyConsumed: Int = 0

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
        if (closed) throw QueryContextClosedException()

        return lock.withLock {
            if (closed) throw QueryContextClosedException()
            action(actionInterface)
        }
    }

    /**
     * Interface to interact with the context. Separated from the other code
     * to 100% enforce thread-safety.
     */
    inner class ActionInterface internal constructor() {
        fun registerConsumptionRequest(command: ConsumeQuerySolutionsCommand) {
            this@QueryContext.consumptionRequests.add(command)
        }

        /**
         * If work can be done on this query, attempts to perform work.
         * @return whether useful work was done
         */
        fun tick(listener: SolutionConsumptionListener): Boolean {
            val consumptionRequest = getNextConsumptionRequest(listener)
            if (closed) {
                return false
            }

            var usefulWorkDone = false

            if (solutions.state == LazySequence.State.PENDING && moreSolutionsRequested(consumptionRequest)) {
                // stepping must only happen in PENDING. If done in RESULTS_AVAILABLE,
                // more solutions are being computed than requested by the client. That
                // would be sub-optimal for read-only queries and absolutely unacceptable for
                // queries with side effects
                solutions.step()
                usefulWorkDone = true
            }

            if (solutions.state == LazySequence.State.RESULTS_AVAILABLE) {
                val nConsumed = consumeSolutions(listener)
                usefulWorkDone = usefulWorkDone || nConsumed > 0
            }

            when(solutions.state) {
                LazySequence.State.FAILED -> {
                    val ex = try {
                        solutions.tryAdvance()
                        null
                    } catch (ex: Exception) {
                        ex
                    }
                    ex!!

                    log.trace("query {} failed", queryId, ex)
                    close()
                    listener.onError(queryId, ex)
                }
                LazySequence.State.DEPLETED -> {
                    log.trace("query {} is depleted of solutions", queryId)
                    close()
                    listener.onSolutionsDepleted(queryId)
                }
                else -> { /* nothing to do */ }
            }

            return usefulWorkDone
        }

        /**
         * If there are open consumption requests (see [registerConsumptionRequest]), consumes
         * available solutions and calls the appropriate methods on the given [listener].
         *
         * It is guaranteed that, before this method returns, all calls to the given
         * listener have returned. In other words: methods of the given listener will not be invoked
         * as a result of an invocation of this method after this method has returned.
         *
         * @return the number of solutions actually consumed
         */
        private fun consumeSolutions(listener: SolutionConsumptionListener): Int {
            var nConsumed = 0
            solutionAvailable@while (solutions.state == LazySequence.State.RESULTS_AVAILABLE) {
                val consumptionRequest = getNextConsumptionRequest(listener)
                if (consumptionRequest == null || closed) {
                    return nConsumed
                }

                while (moreSolutionsRequested(consumptionRequest)) {
                    val solution = solutions.tryAdvance()!!
                    nConsumed++
                    currentlyConsumed++

                    log.trace("obtained one solution to query #{}; handling({}): {}", queryId, consumptionRequest.handling.name, solution)

                    if (consumptionRequest.handling == ConsumeQuerySolutionsCommand.SolutionHandling.RETURN) {
                        listener.onReturnSolution(queryId, solution)
                    }
                }
            }

            return nConsumed
        }

        /**
         * Closes the context, releasing any open resources.
         */
        fun close() {
            log.trace("Closing query #${queryId}")
            closed = true
            solutions.close()
        }

        /**
         * **IMPORTANT:** this method may close the query if a completed consumption request has
         * [ConsumeQuerySolutionsCommand.closeAfterwards] set to true.
         * @return the next consumption request
         */
        private fun getNextConsumptionRequest(listener: SolutionConsumptionListener): ConsumeQuerySolutionsCommand? {
            var currentRequest: ConsumeQuerySolutionsCommand? = consumptionRequests.peek()

            while (currentRequest?.amount != null && currentlyConsumed >= currentRequest.amount!!) {
                if (currentRequest.closeAfterwards) {
                    close()
                    listener.onAbortedByRequest(queryId)
                    return currentRequest
                }

                currentlyConsumed = 0
                consumptionRequests.pop()
                currentRequest = consumptionRequests.peek()
            }

            return currentRequest
        }

        private fun moreSolutionsRequested(currentRequest: ConsumeQuerySolutionsCommand?): Boolean {
            if (currentRequest == null) {
                return false
            }

            if (currentRequest.amount == null) {
                return true
            }

            return currentRequest.amount > currentlyConsumed
        }
    }

    interface SolutionConsumptionListener {
        /**
         * The given solution should be returned to the client as per a
         * [ConsumeQuerySolutionsCommand]
         */
        fun onReturnSolution(queryId: Int, solution: Unification)

        /**
         * Called when the query was closed due to depletion of the
         * [QueryContext.solutions].
         */
        fun onSolutionsDepleted(queryId: Int)

        /**
         * Called after processing a [ConsumeQuerySolutionsCommand] with
         * [ConsumeQuerySolutionsCommand.closeAfterwards] set to `true` and
         * closing the context as a result.
         */
        fun onAbortedByRequest(queryId: Int)

        /**
         * Called after closing the query due to the given error.
         */
        fun onError(queryId: Int, ex: Throwable)
    }
}

class QueryContextClosedException : IllegalStateException()
