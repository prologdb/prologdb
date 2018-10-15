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
interface SessionHandle {
    /**
     * @return waits for the next message incoming in this session.
     *         The incoming message is one of those annotated with [ToServer].
     */
    fun popNextIncomingMessage(): ProtocolMessage

    /**
     * Queues the given message for later sending. The order
     * in which the messages are queued is preserved for the
     * sending.
     */
    fun queueMessage(message: ProtocolMessage)

    /**
     * Directly sends the given message, flushing previously
     * sent messages to preserve order.
     */
    fun sendMessage(message: ProtocolMessage)

    /**
     * Flushes queued outgoing messages.
     * @return the number of messages sent to the client
     */
    fun flushOutbox(): Int

    /**
     * Closes the session.
     */
    fun closeSession()
}