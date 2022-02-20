package com.github.prologdb.indexing

import com.github.prologdb.runtime.PrologException

/**
 * Thrown on indexing errors
 */
class IndexingException(message: String, cause: Throwable? = null) : PrologException(message, cause)