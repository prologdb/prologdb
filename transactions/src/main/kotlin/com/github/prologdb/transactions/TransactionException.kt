package com.github.prologdb.transactions

import com.github.prologdb.runtime.PrologRuntimeException

/**
 * Is thrown for all errors related to transactions.
 */
class TransactionException(message: String, cause: Throwable? = null) : PrologRuntimeException(message, cause)