package com.github.prologdb.net.session

import com.github.prologdb.async.LazySequence
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification

/**
 * To be implemented by applications implementing the prologdb network
 * protocol. This is the interaction between the actual DB and the code
 * responsible for the networking.
 *
 * Implementations **MUST BE THREAD SAFE!**
 */
interface QueryHandler {
    /**
     * @param term the query to run
     * @param totalLimit If not null, the number of results is limited to this number.
     */
    fun startQuery(term: Query, totalLimit: Long?): LazySequence<Unification>

    /**
     * @param command the directive to run
     * @param totalLimit If not null, the number of results is limited to this number.
     */
    fun startDirective(command: Predicate, totalLimit: Long?): LazySequence<Unification>
}