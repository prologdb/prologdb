package com.github.prologdb.execplan

import com.github.prologdb.runtime.PrologRuntimeException

/**
 * Thrown when an execution plan does not match the schema it is executed against.
 */
class PrologQueryException(message: String, cause: Throwable? = null) : PrologRuntimeException(message, cause)