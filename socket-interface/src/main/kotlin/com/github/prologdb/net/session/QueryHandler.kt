package com.github.prologdb.net.session

import com.github.prologdb.runtime.lazysequence.LazySequence
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
    fun startQuery(term: Term): LazySequence<Unification>
    fun startDirective(command: Term): LazySequence<Unification>
}