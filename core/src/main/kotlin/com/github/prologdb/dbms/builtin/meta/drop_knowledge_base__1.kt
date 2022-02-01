package com.github.prologdb.dbms.builtin.meta

import com.github.prologdb.dbms.KnowledgeBaseNotFoundException
import com.github.prologdb.dbms.builtin.nativeDatabaseRule
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.unification.Unification

val BuiltinDropKnowledgeBase1 = nativeDatabaseRule("drop_knowledge_base", 1) { args, ctxt ->
    val name = args.getTyped<Atom>(0).name
    return@nativeDatabaseRule try {
        ctxt.database.dropKnowledgeBase(name)
        Unification.TRUE
    }
    catch (ex: KnowledgeBaseNotFoundException) {
        Unification.FALSE
    }
}