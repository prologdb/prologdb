package com.github.prologdb.indexing

import com.github.prologdb.runtime.PrologRuntimeException

/**
 * Thrown on indexing errors
 */
class IndexingException(message: String, cause: Throwable? = null) : PrologRuntimeException(message, cause)