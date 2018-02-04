package com.github.prologdb.transactions

import com.github.prologdb.runtime.PrologRuntimeException

class TransactionException(message: String, cause: Throwable? = null) : PrologRuntimeException(message, cause)