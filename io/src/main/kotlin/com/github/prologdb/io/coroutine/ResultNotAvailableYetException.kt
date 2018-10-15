package com.github.prologdb.io.coroutine

/**
 * Thrown when a result is to be obtained from a coroutine calculation but that result
 * is not available yet.
 */
class ResultNotAvailableYetException(message: String? = null, cause: Throwable? = null) : RuntimeException(message, cause)