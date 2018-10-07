package com.github.prologdb.net

/**
 * Thrown for all protocol related errors
 */
open class NetworkProtocolException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

class HandshakeFailedException(message: String, cause: Throwable? = null) : NetworkProtocolException(message, cause)