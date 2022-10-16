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
    val fqi = args.getQualifiedIndicator(0, ctxt)
    val options = ImplSelection.parse(args, 1)

    lateinit var uuid: UUID

    runtime.database.dataDirectory.modifySystemCatalog { systemCatalog ->
        val knowledgeBaseCatalog = systemCatalog.knowledgeBases.firstOrNull { it.name == runtime.knowledgeBaseCatalog.name }
            ?: throw KnowledgeBaseNotFoundException(runtime.knowledgeBaseCatalog.name)
        val moduleCatalog = knowledgeBaseCatalog.modulesByName[fqi.moduleName]
        if (moduleCatalog == null) {
            runtime.getLoadedModule(fqi.moduleName) // throws ModuleNotFoundException if it doesn't exist at all
            throw ArgumentError(0, "module ${fqi.moduleName} is not a physical module.")
        }

        if (moduleCatalog.predicates.any { it.functor == fqi.indicator.functor && it.arity == fqi.indicator.arity }) {
            throw DynamicPredicateAlreadyExistsException(fqi)
        }

        uuid = generateSequence { UUID.randomUUID() }
            .first { it !in systemCatalog.allPredicates }

        systemCatalog.copy(knowledgeBases = (systemCatalog.knowledgeBases - knowledgeBaseCatalog) + knowledgeBaseCatalog.copy(
            modules = (knowledgeBaseCatalog.modules - moduleCatalog) + moduleCatalog.copy(
                predicates = moduleCatalog.predicates + SystemCatalog.Predicate(
                    functor = fqi.indicator.functor,
                    arity = fqi.indicator.arity,
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

