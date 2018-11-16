package com.github.prologdb.dbms.builtin

import com.github.prologdb.runtime.PredicateNotDynamicException
import com.github.prologdb.runtime.PrologPermissionError
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification

private val Builtin_Assert_1 = prologdbBuiltin("assert", 1) { args, ctxt ->
    val arg0 = args[0] as? Predicate
        ?: throw PrologRuntimeException("Argument 0 to assert/1 must be a predicate, got ${args[0].prologTypeName}")

    if (arg0.arity == 2 && arg0.name == ":-") {
        TODO("adding rules not implemented yet")
    } else {
        val indicator = ClauseIndicator.of(arg0)
        if (!ctxt.authorization.mayWrite(indicator)) {
            throw PrologPermissionError("Not allowed to write $indicator")
        }

        if (indicator in ctxt.staticBuiltins) {
            throw PredicateNotDynamicException(indicator)
        }

        val store = ctxt.assureFactStore(indicator)
        val pID = await(store.store(ctxt.principal, arg0))
        // TODO: add pID to indices
        yield(Unification.TRUE)
    }
}

/**
 * Provides these builtins:
 * * assert/1
 */
val ModifyLibrary = builtinLibrary("modify", listOf(
    Builtin_Assert_1
))

