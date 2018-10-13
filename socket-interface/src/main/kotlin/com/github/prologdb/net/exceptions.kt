package com.github.prologdb.net

import com.github.prologdb.parser.Reporting

/**
 * Thrown for all protocol related errors
 */
open class NetworkProtocolException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

class HandshakeFailedException(message: String, cause: Throwable? = null) : NetworkProtocolException(message, cause)

class PrologDeserializationException(message: String, cause: Throwable? = null) : NetworkProtocolException(message, cause)

class PrologParseException(val errors: Collection<Reporting>) : NetworkProtocolException("Failed to parse prolog in command")