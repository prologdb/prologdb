package com.github.prologdb.net.session

import com.github.prologdb.async.LazySequence
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.unification.Unification

/**
 * To be implemented by applications implementing the prologdb network
 * protocol. This is the interaction between the actual DB and the code
 * responsible for the networking.
 *
 * Implementations need not be thread-safe.
 */
interface QueryHandler {
    /**
     * @param term the query to run
     * @param totalLimit If not null, the number of results is limited to this number.
     */
    fun startQuery(term: Term, totalLimit: Long?): LazySequence<Unification>

    /**
     * @param command the directive to run
     * @param totalLimit If not null, the number of results is limited to this number.
     */
    fun startDirective(command: Term, totalLimit: Long?): LazySequence<Unification>
}