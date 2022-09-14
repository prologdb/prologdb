package com.github.prologdb.dbms.builtin.meta.global

import com.github.prologdb.dbms.builtin.nativeDatabaseRule
import com.github.prologdb.runtime.term.Atom

val BuiltinCreateKnowledgeBase1 = nativeDatabaseRule("create_knowledge_base", 1) { args, ctxt ->
    val name = args.getTyped<Atom>(0).name

    ctxt.runtimeEnvironment.database.createKnowledgeBase(name)
    Unification.TRUE
}