package com.github.prologdb.net.session

import com.github.prologdb.async.LazySequence
import com.github.prologdb.net.session.handle.ParserDelegate
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.Unification

/**
 * To be implemented by applications implementing the prologdb network
 * protocol. This is the interaction between the actual DB engine and the code
 * responsible for the networking.
 *
 * @param SessionState State that is associated with every session/connection open to the
 *                     server.
 *
 * Implementations **MUST BE THREAD SAFE!**
 */
interface DatabaseEngine<SessionState : Any> : ParserDelegate<SessionState> {
    /**
     * Called when a new connection is opened. This is the opportunity for the engine
     * to allocate context for that connection.
     */
    fun initializeSession(): SessionState

    /**
     * Called when a connection is closed. This is the opportunity to do teardown work.
     * If possible, the teardown work should be done on another thread to improve responsiveness
     * of the network interface.
     */
    fun onSessionDestroyed(state: SessionState)

    /**
     * @param query the query to run
     */
    fun startQuery(session: SessionState, query: Query): LazySequence<Unification>

    /**
     * @param command the directive to run
     */
    fun startDirective(session: SessionState, command: CompoundTerm): LazySequence<Unification>
}