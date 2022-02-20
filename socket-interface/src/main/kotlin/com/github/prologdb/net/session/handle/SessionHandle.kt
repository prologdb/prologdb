package com.github.prologdb.net.session.handle

import com.github.prologdb.net.session.ProtocolMessage
import io.reactivex.Observable
import java.net.Socket

/**
 * A handle of a connection/session. Reference to the actual [Socket] along with the
 * information relevant to that session.
 * These objects also take care of handling the actual protocol version.
 *
 * @param SessionState Session state as obtained and managed by the [DatabaseEngine].
 *
 * Session handles are generally **NOT THREAD SAFE!**
 */
interface SessionHandle<SessionState : Any> {
    /**
     * The incoming message is one of those annotated with [ToServer].
     */
    val incomingMessages: Observable<ProtocolMessage>

    /**
     * Simple storage variable. Implementations supposed not to tinker with this object.
     */
    var sessionState: SessionState?

    /**
     * An identifier for this very connection to the server.
     */
    val clientId: String

    /**
     * Queues the given message for later sending. The order
     * in which the messages are queued is preserved for the
     * sending.
     */
    fun queueMessage(message: ProtocolMessage)

    /**
     * Closes the session.
     */
    fun closeSession()
}