package com.github.prologdb.indexing

import com.github.prologdb.runtime.PrologException

/**
 * Thrown on indexing errors
 */
class IndexingException(message: String, cause: Throwable? = null) : PrologException(message, cause)

/**
 * Thrown when a supplied key cannot be used to search within or write to an index.
 */
class InvalidIndexKeyException(message: String, cause: Throwable? = null) : IndexingException(message, cause)