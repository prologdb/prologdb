package com.github.prologdb.client

import com.github.prologdb.async.IrrelevantPrincipal
import com.github.prologdb.async.LazySequence
import com.github.prologdb.net.v1.messages.QueryClosedEvent
import com.github.prologdb.net.v1.messages.QueryRelatedError
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.unification.Unification
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue

class RemoteSolutions(
    private val queryId: Int,
    private val onMoreSolutionsNeeded: () -> Unit,
    private val onAbort: () -> Unit
) : LazySequence<Unification> {
    override val principal = IrrelevantPrincipal

    // not affected by [mutex]
    private val solutionQueue =  LinkedBlockingQueue<Unification>()

    private val mutex = Any()

    private var closed = false
    private var closeReason: QueryClosedEvent.Reason? = null

    private val advancementNotifier = Object()

    /**
     * Completes when this query errors. Completes exceptionally
     * when the query is closed or depletes without failing.
     */
    private var error = CompletableFuture<QueryRelatedError>()

    override val state: LazySequence.State
        get() {
            if (solutionQueue.isNotEmpty()) return LazySequence.State.RESULTS_AVAILABLE
            return if (!closed) LazySequence.State.PENDING else when(closeReason!!) {
                QueryClosedEvent.Reason.ABORTED_ON_USER_REQUEST,
                QueryClosedEvent.Reason.FAILED -> LazySequence.State.FAILED
                QueryClosedEvent.Reason.SOLUTIONS_DEPLETED -> LazySequence.State.DEPLETED
            }
        }

    override fun close() {
        onAbort()
    }

    override fun step() = state

    override tailrec fun tryAdvance(): Unification? {
        synchronized(mutex) {
            if (solutionQueue.isNotEmpty()) {
                // println("tryAdvance invoked, solutions not empty, yielding...")
                return solutionQueue.take()
            }

            if (closed) {
                // println("tryAdvance invoked, sequence closed, handling...")
                when (closeReason) {
                    QueryClosedEvent.Reason.ABORTED_ON_USER_REQUEST -> throw PrologRuntimeException("Query $queryId aborted on user request")
                    QueryClosedEvent.Reason.FAILED -> throw error.join().toException()
                    QueryClosedEvent.Reason.SOLUTIONS_DEPLETED -> return null
                }
            }
        }

        onMoreSolutionsNeeded()

        synchronized(advancementNotifier) {
            while (solutionQueue.isEmpty() && !error.isDone) {
                // println("tryAdvance invoked, waiting for solution or error...")
                try {
                    advancementNotifier.wait()
                }
                catch (ex: InterruptedException) {}
            }
        }

        // println("got notify()d, retrying to get a solution...")
        return tryAdvance()
    }

    internal fun onOpened() {
        // nothing to do, really
    }

    internal fun onClosed(reason: QueryClosedEvent.Reason) {
        synchronized(mutex) {
            this.closed = true
            this.closeReason = reason
            synchronized(advancementNotifier) {
                advancementNotifier.notifyAll()
            }

            when (reason) {
                QueryClosedEvent.Reason.SOLUTIONS_DEPLETED -> error.completeExceptionally(RuntimeException("Query depleted without error"))
                QueryClosedEvent.Reason.ABORTED_ON_USER_REQUEST -> error.complete(QueryRelatedError.newBuilder()
                    .setShortMessage("Query $queryId aborted on user request")
                    .setKind(QueryRelatedError.Kind.ERROR_GENERIC)
                    .setQueryId(queryId)
                    .build()
                )
            }

            Unit
        }
    }

    internal fun onSolution(solution: Unification) {
        solutionQueue.put(solution)

        synchronized(advancementNotifier) {
            advancementNotifier.notifyAll()
        }
    }

    internal fun onError(error: QueryRelatedError) {
        // println("Got an error: $error, waiting on mutex notify()ing ")
        synchronized(mutex) {
            this.closed = true
            this.closeReason = QueryClosedEvent.Reason.FAILED
            this.error.complete(error)
        }

        // println("Got an error: $error, notify()ing ")
        synchronized(advancementNotifier) {
            advancementNotifier.notifyAll()
        }
    }
}

private fun QueryRelatedError.toException(): Throwable {
    return when (this.kind!!) {
        QueryRelatedError.Kind.QUERY_ID_ALREADY_IN_USE,
        QueryRelatedError.Kind.QUERY_ID_NOT_IN_USE,
        QueryRelatedError.Kind.INVALID_TERM_SYNTAX,
        QueryRelatedError.Kind.ERROR_GENERIC -> GenericQueryError(this.shortMessage, this.additionalInformationMap)
        QueryRelatedError.Kind.ERROR_CONSTRAINT_VIOLATION,
        QueryRelatedError.Kind.ERROR_PREDICATE_NOT_DYNAMIC -> QueryError(this.shortMessage, this.additionalInformationMap)
    }
}