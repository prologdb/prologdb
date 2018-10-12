package com.github.prologdb.net.session.handle

import java.net.Socket

/**
 * A handle of a connection/session. Reference to the actual [Socket] along with the
 * information relevant to that session.
 * These objects also take care of handling the actual protocol version.
 */
sealed class SessionHandle {

}

internal class ProtocolVersion1SessionHandle(
    private val socket: Socket
)