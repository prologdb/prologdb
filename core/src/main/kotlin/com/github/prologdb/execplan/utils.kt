package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.transformExceptionsOnRemaining
import com.github.prologdb.runtime.PrologException
import com.github.prologdb.runtime.exception.PrologStackTraceElement

/**
 * Maps the sequence; for every [PrologException] thrown from the original sequence as a result
 * of invocations to [LazySequence.tryAdvance] the [PrologException.prologStackTrace] is amended
 * with the given stack trace element (as if each call to `tryAdvance` were wrapped in [prologTry]).
 *
 * This has to be included in all calls made by the interpreter that should show up in the prolog
 * stacktraces; this is definitely for all calls the interpreter starts on behalf of user code.
 * Calls the interpreter does on its own behalf **may** use this.
 */
fun <T : Any> LazySequence<T>.amendExceptionsWithStackTraceOnRemaining(onErrorStackTraceElement: PrologStackTraceElement): LazySequence<T> {
    return this.transformExceptionsOnRemaining { e: PrologException ->
        e.addPrologStackFrame(onErrorStackTraceElement)
        e
    }
}

