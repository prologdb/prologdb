package com.github.prologdb.client

import com.github.prologdb.async.IrrelevantPrincipal
import com.github.prologdb.async.LazySequence
import com.github.prologdb.net.v1.messages.QueryRelatedError
import java.util.concurrent.LinkedBlockingQueue
import com.github.prologdb.net.v1.messages.QueryClosedEvent as NetQClosedEvent

class RemoteSolutions internal constructor(
    private val queryId: Int,
    private val prefetchAmount: Int,
    private val onMoreSolutionsNeeded: (Int) -> Unit,
    private val onAbort: () -> Unit
) : LazySequence<Unification> {
    init {
        assert(prefetchAmount >= 1)
    }

    private val eventQueue = LinkedBlockingQueue<QueryEvent>()

    override val principal = IrrelevantPrincipal

    internal var closed = false
        private set
    private var closeReason: NetQClosedEvent.Reason? = null

    /**
     * Set true after solutions have been requested the first time
     */
    private var firstSolutionRequestSent = false

    /**
     * Set true when the first solution was received (or if there are no
     * solutions, when the close event is received).
     */
    @Volatile
    private var firstSolutionReceived = false

    @Volatile
    private var solutionRequestUnderway = false

    override val state: LazySequence.State
        get() {
            if (closed) {
                return if (closeReason == null) LazySequence.State.DEPLETED else closeReason!!.asLazySequenceSate()
            }

            val nextEvent = eventQueue.peek()
            return when (nextEvent) {
                null                  -> LazySequence.State.PENDING
                is QuerySolutionEvent -> LazySequence.State.RESULTS_AVAILABLE
                is QueryErrorEvent    -> LazySequence.State.FAILED
                is QueryClosedEvent   -> nextEvent.reason.asLazySequenceSate()
            }
        }

    override fun close() {
        onAbort()
    }

    override fun step(): LazySequence.State {
        if (state == LazySequence.State.PENDING) {
            onMoreSolutionsNeeded()
        }

        return state
    }

    /**
     * When [tryAdvance] consumes an [QueryErrorEvent], this
     * is set to the error so subsequent invocations to
     * [tryAdvance] can throw that error again.
     */
    private var errorCache: Throwable? = null
    override fun tryAdvance(): Unification? {
        if (closed) {
            when (closeReason) {
                NetQClosedEvent.Reason.ABORTED_ON_USER_REQUEST -> throw QueryClosedOnUserRequestException(queryId)
                NetQClosedEvent.Reason.FAILED -> throw errorCache!!
                NetQClosedEvent.Reason.SOLUTIONS_DEPLETED -> return null
                null -> throw IllegalStateException("Missing closeReason")
            }
        }

        if (eventQueue.isEmpty()) {
            onMoreSolutionsNeeded()
        }

        val event = eventQueue.take()

        return when (event) {
            is QuerySolutionEvent -> {
                event.solution
            }
            is QueryClosedEvent   -> {
                closeReason = event.reason
                closed = true
                if (event.reason == NetQClosedEvent.Reason.FAILED) {
                    throw errorCache!!
                } else null
            }
            is QueryErrorEvent    -> {
                val error = event.error.toException()
                errorCache = error
                throw error
            }
        }
    }

    private fun onMoreSolutionsNeeded() {
        if (closed) return

        if (firstSolutionRequestSent) {
            if (firstSolutionReceived) {
                if (solutionRequestUnderway) {
                    // already underway, wait
                    return
                } else {
                    // one more at a time
                    onMoreSolutionsNeeded(1)
                    solutionRequestUnderway = true
                }
            } else {
                // the first request for the prefetch amount is still
                // in progress; just wait longer
                return
            }
        } else  {
            // first, request the prefetch amount
            firstSolutionRequestSent = true
            solutionRequestUnderway = true
            onMoreSolutionsNeeded(prefetchAmount)
        }
    }

    internal fun onQueryEvent(event: QueryEvent) {
        eventQueue.put(event)
        if (event is QuerySolutionEvent) {
            if (!firstSolutionReceived) {
                firstSolutionReceived = true
            }
            solutionRequestUnderway = false
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

private fun NetQClosedEvent.Reason.asLazySequenceSate(): LazySequence.State = when(this) {
    NetQClosedEvent.Reason.ABORTED_ON_USER_REQUEST -> LazySequence.State.DEPLETED
    NetQClosedEvent.Reason.FAILED                  -> LazySequence.State.FAILED
    NetQClosedEvent.Reason.SOLUTIONS_DEPLETED      -> LazySequence.State.DEPLETED
}