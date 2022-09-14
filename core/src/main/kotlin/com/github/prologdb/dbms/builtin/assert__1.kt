package com.github.prologdb.dbms.builtin

import com.github.prologdb.runtime.PrologInvocationContractViolationException

val BuiltinDatabaseAssert1 = nativeDatabaseRule("assert", 1) { args, ctxt ->
    val (fqi, callable, resolvedHead) = ctxt.resolveHead(args.getTyped(0))
    if (callable !is PhysicalDynamicPredicate) {
        throw PrologInvocationContractViolationException("Cannot assert clauses to $fqi: not a physical, dynamic predicate.")
    }

    await(ctxt.getFactStore(callable.catalog).store(ctxt.principal, resolvedHead.arguments))
    Unification.TRUE
}