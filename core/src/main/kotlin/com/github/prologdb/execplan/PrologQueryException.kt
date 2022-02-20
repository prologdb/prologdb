package com.github.prologdb.execplan

import com.github.prologdb.runtime.PrologException

/**
 * Thrown when an error occcurs during the execution of a query (-plan)
 */
open class PrologQueryException(message: String, cause: Throwable? = null) : PrologException(message, cause)