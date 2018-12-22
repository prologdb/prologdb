package com.github.prologdb.indexing

import com.github.prologdb.runtime.PrologRuntimeException

/**
 * Thrown on indexing errors
 */
open class IndexingException(message: String, cause: Throwable? = null) : PrologRuntimeException(message, cause)

/**
 * Thrown when a supplied key cannot be used to search within or write to an index.
 */
class InvalidIndexKeyException(message: String, cause: Throwable? = null) : IndexingException(message, cause)