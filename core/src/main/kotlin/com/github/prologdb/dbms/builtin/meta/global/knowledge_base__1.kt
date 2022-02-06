package com.github.prologdb.dbms.builtin.meta.global

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.mapRemainingNotNull
import com.github.prologdb.dbms.builtin.nativeDatabaseRule
import com.github.prologdb.runtime.ArgumentTypeError
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.Variable

val BuiltinKnowledgeBase1 = nativeDatabaseRule("knowledge_base", 1) { args, ctxt ->
    val nameArg = args[0]
    if (nameArg !is Variable && nameArg !is Atom) {
        throw ArgumentTypeError(null, 0, nameArg, Variable::class.java, Atom::class.java)
    }

    yieldAllFinal(
        LazySequence.ofIterable(ctxt.database.dataDirectory.systemCatalog.knowledgeBases)
            .mapRemainingNotNull { Atom(it.name).unify(nameArg, ctxt.randomVariableScope) }
    )
}