package com.github.prologdb.dbms.builtin.meta

import com.github.prologdb.dbms.KnowledgeBaseNotFoundException
import com.github.prologdb.dbms.MetaKnowledgeBaseRuntimeEnvironment
import com.github.prologdb.dbms.SystemCatalog
import com.github.prologdb.dbms.builtin.nativeDatabaseRule
import com.github.prologdb.indexing.IndexTemplate
import com.github.prologdb.runtime.ArgumentError
import com.github.prologdb.runtime.PrologInvocationContractViolationException
import com.github.prologdb.runtime.stdlib.TypedPredicateArguments
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.PrologList
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.Unification
import java.util.*

val BuiltinCreateIndex5 = nativeDatabaseRule("create_index", 5) { args, ctxt ->
    val runtime = ctxt.runtimeEnvironment as? MetaKnowledgeBaseRuntimeEnvironment
        ?: throw PrologInvocationContractViolationException("${args.indicator} must be invoked within a meta knowledge base.")

    val name = args.getTyped<Atom>(0).name
    val moduleName = args.getTyped<Atom>(1).name
    val templateGoal = args.getQuery(2)
    val config = args.getIndexConfig(3)

    val template = IndexTemplate(moduleName, templateGoal)
    val implSelection = ImplSelection.parse(args, 4)
    lateinit var uuid: UUID

    runtime.database.dataDirectory.modifySystemCatalog { systemCatalog ->
        val knowledgeBaseCatalog = systemCatalog.knowledgeBases.firstOrNull { it.name == runtime.knowledgeBaseCatalog.name }
            ?: throw KnowledgeBaseNotFoundException(runtime.knowledgeBaseCatalog.name)

        val moduleCatalog = knowledgeBaseCatalog.modulesByName[moduleName]
        if (moduleCatalog == null) {
            runtime.getLoadedModule(moduleName) // throws ModuleNotFoundException if it doesn't exist at all
            throw ArgumentError(1, "module $moduleName is not a physical module.")
        }

        val predicateCatalog = moduleCatalog.predicates
            .find { it.indicator == template.baseFactIndicator }
            ?: throw ArgumentError(2, "there is no physical predicate ${template.baseFactIndicator}, can only index on physical predicates")

        if (moduleCatalog.predicates.flatMap { it.indices }.any { it.name == name }) {
            throw ArgumentError(0, "an index with name $name is already defined in module ${moduleCatalog.name}")
        }

        uuid = generateSequence { UUID.randomUUID() }
            .first { it !in systemCatalog.allIndices }

        systemCatalog.withModifiedPredicate(predicateCatalog.uuid) { _ ->
            predicateCatalog.copy(indices = predicateCatalog.indices + setOf(
                SystemCatalog.Index(
                    name,
                    uuid,
                    template.unscopedTemplate,
                    config.key,
                    config.storeAdditionally,
                    null,
                )
            ))
        }
    }

    when (implSelection) {
        is ImplSelection.Fixed -> runtime.database.createFactIndex(uuid, implSelection.id)
        is ImplSelection.ByFeatureSupport -> runtime.database.createFactIndex(uuid, implSelection.required, implSelection.desired)
    }

    Unification.TRUE
}

private data class IndexConfig(
    val key: SortedSet<Variable>,
    val storeAdditionally: Set<Variable>,
)

private fun TypedPredicateArguments.getIndexConfig(argIndex: Int): IndexConfig {
    val configOptions = getTyped<PrologList>(argIndex)
    if (configOptions.tail != null) {
        throw ArgumentError(argIndex, "Cannot have a tail")
    }

    var key: SortedSet<Variable>? = null
    var store: Set<Variable>? = null
    for (option in configOptions.elements) {
        if (option !is CompoundTerm) {
            throw ArgumentError(argIndex, "all options must be compounds, got ${option.prologTypeName}")
        }

        when (option.functor) {
            "key" -> {
                if (key != null) {
                    throw ArgumentError(argIndex, "the key option can only be given once")
                }

                key = option.arguments
                    .map { it as? Variable ?: throw ArgumentError(argIndex, "all entries in the key option must be unbound, got ${it.prologTypeName}") }
                    .toSortedSet()
            }
            "store" -> {
                if (store != null) {
                    throw ArgumentError(argIndex, "the store option can only be given once")
                }

                store = option.arguments
                    .map { it as? Variable ?: throw ArgumentError(argIndex, "all entries in the store option must be unbound, got ${it.prologTypeName}") }
                    .toSet()
            }
        }
    }

    if (key.isNullOrEmpty()) {
        throw ArgumentError(argIndex, "no index keys were specified; at least one key is required")
    }

    return IndexConfig(key, store ?: emptySet())
}