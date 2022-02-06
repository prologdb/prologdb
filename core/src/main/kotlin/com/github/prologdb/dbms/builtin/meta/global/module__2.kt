package com.github.prologdb.dbms.builtin.meta.global

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.async.mapRemainingNotNull
import com.github.prologdb.dbms.builtin.nativeDatabaseRule
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.unify

val BuiltinModule2 = nativeDatabaseRule("module", 2) { args, ctxt ->
    val systemCatalog = ctxt.runtimeEnvironment.database.dataDirectory.systemCatalog

    yieldAllFinal(
        LazySequence.ofIterable(systemCatalog.knowledgeBases)
            .flatMapRemaining { knowledgeBaseCatalog -> yieldAllFinal(
                LazySequence.ofIterable(knowledgeBaseCatalog.modules)
                    .mapRemainingNotNull { module ->
                        args.raw.unify(arrayOf(Atom(knowledgeBaseCatalog.name), Atom(module.name)), ctxt.randomVariableScope)
                    }
            ) }
    )
}