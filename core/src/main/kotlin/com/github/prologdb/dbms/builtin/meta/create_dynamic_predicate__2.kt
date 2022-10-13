package com.github.prologdb.dbms.builtin.meta

import com.github.prologdb.dbms.DynamicPredicateAlreadyExistsException
import com.github.prologdb.dbms.KnowledgeBaseNotFoundException
import com.github.prologdb.dbms.MetaKnowledgeBaseRuntimeEnvironment
import com.github.prologdb.dbms.SystemCatalog
import com.github.prologdb.dbms.builtin.nativeDatabaseRule
import com.github.prologdb.runtime.ArgumentError
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.FullyQualifiedClauseIndicator
import com.github.prologdb.runtime.PrologInvocationContractViolationException
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.PrologNumber
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.Unification
import java.util.UUID

val BuiltinCreateDynamicPredicate2 = nativeDatabaseRule("create_dynamic_predicate", 2) { args, ctxt ->
    val runtime = ctxt.runtimeEnvironment as? MetaKnowledgeBaseRuntimeEnvironment
        ?: throw PrologInvocationContractViolationException("${args.indicator} must be invoked within a meta knowledge base.")

    val inputs = FQI_INDICATOR_TEMPLATE.unify(args[0], ctxt.randomVariableScope)
        ?.entries
        ?.associate { it.first.name to it.second }
        ?: throw ArgumentError(0, "must be a fully qualified indicator of this structure: $FQI_INDICATOR_TEMPLATE")

    val moduleName = inputs["Module"] as? Atom
        ?: throw ArgumentError(0, "must specify the module as an atom")
    val functor = inputs["Functor"] as? Atom
        ?: throw ArgumentError(0, "must specify the functor as an atom")
    val arityTerm = inputs["Arity"] as? PrologNumber
        ?: throw ArgumentError(0, "must specify the arity as an integer")
    val arity = arityTerm
        .takeIf { it > PrologNumber(0) && it <= PrologNumber(Int.MAX_VALUE) }
        ?.toInteger()
        ?.toInt()
        ?: throw ArgumentError(0, "must specify an arity beteen 1 and ${Int.MAX_VALUE}")
    val options = ImplSelection.parse(args, 1)

    val fqi = FullyQualifiedClauseIndicator(moduleName.name, ClauseIndicator.of(functor.name, arity))
    lateinit var uuid: UUID

    runtime.database.dataDirectory.modifySystemCatalog { systemCatalog ->
        val knowledgeBaseCatalog = systemCatalog.knowledgeBases.firstOrNull { it.name == runtime.knowledgeBaseCatalog.name }
            ?: throw KnowledgeBaseNotFoundException(runtime.knowledgeBaseCatalog.name)
        val moduleCatalog = knowledgeBaseCatalog.modulesByName[moduleName.name]
        if (moduleCatalog == null) {
            runtime.getLoadedModule(moduleName.name) // throws ModuleNotFoundException if it doesn't exist at all
            throw ArgumentError(0, "module $moduleName is not a physical module.")
        }

        if (moduleCatalog.predicates.any { it.functor == functor.name && it.arity == arity }) {
            throw DynamicPredicateAlreadyExistsException(fqi)
        }

        uuid = generateSequence { UUID.randomUUID() }
            .first { it !in systemCatalog.allPredicates }

        systemCatalog.copy(knowledgeBases = (systemCatalog.knowledgeBases - knowledgeBaseCatalog) + knowledgeBaseCatalog.copy(
            modules = (knowledgeBaseCatalog.modules - moduleCatalog) + moduleCatalog.copy(
                predicates = moduleCatalog.predicates + SystemCatalog.Predicate(
                    functor = functor.name,
                    arity = arity,
                    uuid = uuid,
                    factStoreImplementationId = null
                )
            )
        ))
    }

    when(options) {
        is ImplSelection.Fixed -> runtime.database.createFactStore(uuid, options.id)
        is ImplSelection.ByFeatureSupport -> runtime.database.createFactStore(uuid, options.required, options.desired)
    }

    Unification.TRUE
}

private val FQI_INDICATOR_TEMPLATE = CompoundTerm(":", arrayOf(
    Variable("Module"),
    CompoundTerm("/", arrayOf(
        Variable("Functor"),
        Variable("Arity")
    ))
))

