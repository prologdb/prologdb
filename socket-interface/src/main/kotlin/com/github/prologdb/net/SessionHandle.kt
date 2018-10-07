package com.github.prologdb.net

import java.net.Socket

/**
 * A handle of a connection/session. Reference to the actual [Socket] along with the
 * information relevant to that session.
 * These objects also take care of handling the actual protocol version.
 */
sealed class SessionHandle {
    companion object {
        /**
         * Performs the handshake with the oder side and initializes a suitable
         * [SessionHandle] instance for the negotiated parameters.
         */
        fun init(socket: Socket): SessionHandle {
            TODO()
        }
    }
}

internal class ProtocolVersion1SessionHandle(
    private val socket: Socket
)