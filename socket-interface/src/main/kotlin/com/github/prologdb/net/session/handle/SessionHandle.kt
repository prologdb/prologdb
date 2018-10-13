package com.github.prologdb.net.session.handle

import com.github.prologdb.net.session.ProtocolMessage
import java.net.Socket

/**
 * A handle of a connection/session. Reference to the actual [Socket] along with the
 * information relevant to that session.
 * These objects also take care of handling the actual protocol version.
 *
 * Session handles are generally **NOT THREAD SAFE!**
 */
abstract class SessionHandle {
    /**
     * @return waits for the next message incoming in this session.
     *         The incoming message is one of those annotated with [ToServer].
     */
    abstract fun popNextIncomingMessage(): ProtocolMessage

    /**
     * Sends the given message to the session client. Implementations may
     * queue messages and send them later.
     */
    abstract fun sendMessage(message: ProtocolMessage)

    /**
     * Closes the session.
     */
    abstract fun closeSession()
}