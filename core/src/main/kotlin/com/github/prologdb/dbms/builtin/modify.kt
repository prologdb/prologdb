package com.github.prologdb.dbms.builtin

import com.github.prologdb.runtime.PredicateNotDynamicException
import com.github.prologdb.runtime.PrologPermissionError
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.Unification

private val Builtin_Assert_1 = databaseBuiltin("assert", 1) { args, ctxt ->
    val arg0 = args[0] as? CompoundTerm
        ?: throw PrologRuntimeException("Argument 1 to assert/1 must be a compound term, got ${args[0].prologTypeName}")

    if (arg0.arity == 2 && arg0.functor == ":-") {
        throw PrologRuntimeException("Cannot assert rules; you need to declare them in a library.")
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
val ModifyLibrary = databaseCompatibleLibrary("modify") {
    add(Builtin_Assert_1)
}

