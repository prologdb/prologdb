package com.github.prologdb.client

import com.github.prologdb.runtime.PrologRuntimeException

open class PrologDBClientException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

class GenericQueryError(
    message: String,
    val additionalInformation: Map<String, String>
) : PrologDBClientException(message)

class QueryError(
    message: String,
    val additionalInformation: Map<String, String>
) : PrologRuntimeException(message)