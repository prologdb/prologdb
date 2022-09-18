package com.github.prologdb.indexing

import com.github.prologdb.runtime.PrologException

/**
 * Thrown on indexing errors
 */
open class IndexingException(message: String, cause: Throwable? = null) : PrologException(message, cause)

/**
 * Thrown when a supplied key cannot be used to search within or write to an index.
 */
class InvalidIndexKeyException(message: String, cause: Throwable? = null) : IndexingException(message, cause)

/**
 * Thrown when an index is referenced (e.g. by name) but such an index is not found.
 */
class IndexNotFoundException(message: String, cause: Throwable? = null): IndexingException(message, cause)