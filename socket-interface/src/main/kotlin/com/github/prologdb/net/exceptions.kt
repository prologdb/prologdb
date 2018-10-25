package com.github.prologdb.net

import com.github.prologdb.net.session.QueryRelatedError
import com.github.prologdb.parser.parser.ParseResult

/**
 * Thrown for all protocol related errors
 */
open class NetworkProtocolException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

class HandshakeFailedException(message: String, cause: Throwable? = null) : NetworkProtocolException(message, cause)

class PrologDeserializationException(message: String, cause: Throwable? = null) : NetworkProtocolException(message, cause)

class PrologParseException(result: ParseResult<*>) : NetworkProtocolException("Failed to parse prolog in command") {
    val errors = result.reportings
}

class QueryRelatedException(val errorObject: QueryRelatedError, cause: Throwable? = null) : NetworkProtocolException(
    errorObject.shortMessage ?: errorObject.kind.name,
    cause
)