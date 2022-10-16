package com.github.prologdb.dbms.builtin

import com.github.prologdb.async.*
import com.github.prologdb.runtime.PrologInvocationContractViolationException
import com.github.prologdb.runtime.unification.Unification

val BuiltinDatabaseAssert1 = nativeDatabaseRule("assert", 1) { args, ctxt ->
    val (fqi, callable, resolvedHead) = ctxt.resolveHead(args.getTyped(0))
    if (callable !is PhysicalDynamicPredicate) {
        throw PrologInvocationContractViolationException("Cannot assert clauses to $fqi: not a physical, dynamic predicate.")
    }

    val persistenceId = await(ctxt.getFactStore(callable.catalog).store(ctxt.principal, resolvedHead.arguments))
    for (indexCatalog in callable.catalog.indices) {
        val factIndex = ctxt.getFactIndex(indexCatalog)
        val entriesSequence = factIndex.definition.template.getEntriesFor(
            resolvedHead,
            persistenceId,
            ctxt,
        )

        await(
            entriesSequence
                .flatMapRemaining {
                    await(factIndex.onInserted(it, ctxt.principal))
                    Unification.FALSE
                }
                .consumeAll()
        )
    }
    Unification.TRUE
}