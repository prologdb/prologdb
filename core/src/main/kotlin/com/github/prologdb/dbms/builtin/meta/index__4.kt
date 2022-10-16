package com.github.prologdb.dbms.builtin.meta

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.mapRemainingNotNull
import com.github.prologdb.dbms.MetaKnowledgeBaseRuntimeEnvironment
import com.github.prologdb.dbms.builtin.nativeDatabaseRule
import com.github.prologdb.runtime.PrologInvocationContractViolationException
import com.github.prologdb.runtime.query.AndQuery
import com.github.prologdb.runtime.query.OrQuery
import com.github.prologdb.runtime.query.PredicateInvocationQuery
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.*
import com.github.prologdb.runtime.unification.Unification

val BuiltinIndex4 = nativeDatabaseRule("index", 4) { args, ctxt ->
    val runtime = ctxt.runtimeEnvironment as? MetaKnowledgeBaseRuntimeEnvironment
        ?: throw PrologInvocationContractViolationException("${args.indicator} must be invoked within a meta knowledge base.")

    val moduleName = ctxt.module.declaration.moduleName
    val moduleCatalog = runtime.knowledgeBaseCatalog.modules.find { it.name == moduleName }
        ?: return@nativeDatabaseRule Unification.FALSE

    return@nativeDatabaseRule yieldAllFinal(
        LazySequence.ofIterable(moduleCatalog.predicates.flatMap { it.indices })
            .mapRemainingNotNull { indexCatalog ->
                val nameTerm = Atom(indexCatalog.name)
                val templateGoalTerm = indexCatalog.unscopedTemplateGoal.toTerm()
                val keyTerm = CompoundTerm("key", indexCatalog.key.toTypedArray())
                val storeTerm = indexCatalog.storeAdditionally
                    .takeUnless { it.isEmpty() }
                    ?.let { CompoundTerm("store", it.toTypedArray()) }
                val configTerm = PrologList(listOfNotNull(keyTerm, storeTerm))
                val optionsTerm = PrologList(listOfNotNull(
                    indexCatalog.factIndexImplementationId?.let { CompoundTerm("impl", arrayOf(Atom(it))) }
                ))

                arrayOf(nameTerm, templateGoalTerm, configTerm, optionsTerm).unify(args.raw, ctxt.randomVariableScope)
            }
    )
}

private fun Query.toTerm(): Term = when(this) {
    is PredicateInvocationQuery -> goal
    is AndQuery -> goals.toList().toTerm(",")
    is OrQuery -> goals.toList().toTerm(";")
}

private fun List<Query>.toTerm(functor: String): Term = when(size) {
    0 -> CompoundTerm(functor, emptyArray())
    1 -> first().toTerm()
    else -> CompoundTerm(functor, arrayOf(first().toTerm(), subList(1, size).toTerm(functor)))
}