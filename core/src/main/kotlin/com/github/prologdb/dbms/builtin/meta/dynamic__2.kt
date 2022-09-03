package com.github.prologdb.dbms.builtin.meta

import com.github.prologdb.dbms.MetaKnowledgeBaseRuntimeEnvironment
import com.github.prologdb.dbms.builtin.nativeDatabaseRule
import com.github.prologdb.parser.ParseException
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.runtime.ArgumentTypeError
import com.github.prologdb.runtime.PrologInternalError
import com.github.prologdb.runtime.PrologInvocationContractViolationException
import com.github.prologdb.runtime.term.*

private val PARSER = PrologParser()

val BuiltinDynamic2 = nativeDatabaseRule("dynamic", 2) { args, ctxt ->
    val runtime = ctxt.runtimeEnvironment as? MetaKnowledgeBaseRuntimeEnvironment
        ?: throw PrologInvocationContractViolationException("${args.indicator} must be invoked in a meta knowledge-base.")

    var predicates = ctxt.runtimeEnvironment.database.dataDirectory.systemCatalog.knowledgeBases
        .asSequence()
        .filter { kb -> kb.name == runtime.knowledgeBaseCatalog.name }
        .map { kb -> kb.modulesByName.getValue(ctxt.module.declaration.moduleName) }
        .flatMap { module -> module.predicates }

    when (args[0]) {
        is Variable -> {
        }
        is CompoundTerm -> {
            val result = PARSER.parseIdiomaticClauseIndicator(args[0])
            ParseException.failOnError(result.reportings)
            val indicator = result.item
                ?: throw PrologInternalError("Failed to parse clause indicator. Got no errors and no result.")

            predicates = predicates.filter { p -> p.indicator.indicator == indicator }
        }
        else -> throw ArgumentTypeError(0, args[0], CompoundTerm::class.java, Variable::class.java)
    }

    return@nativeDatabaseRule yieldAllFinal(predicates
        .mapNotNull { predicate ->
            val indicatorTerm = predicate.indicator.indicator.toIdiomatic()
            val optionsTerm = PrologList(
                predicate.factStoreImplementationId
                    ?.let { listOf(Atom(it)) }
                    ?: emptyList()
            )

            arrayOf(indicatorTerm, optionsTerm).unify(arrayOf(args[0], args[1]), ctxt.randomVariableScope)
        }
    )
}