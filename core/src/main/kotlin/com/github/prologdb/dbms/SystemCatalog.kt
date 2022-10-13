package com.github.prologdb.dbms

import com.fasterxml.jackson.annotation.JsonIgnore
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.FullyQualifiedClauseIndicator
import com.github.prologdb.runtime.HasFunctorAndArity
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Variable
import java.util.UUID

data class SystemCatalog(
    @JsonIgnore
    val revision: Long,
    val knowledgeBases: Set<KnowledgeBase>
) {
    @get:JsonIgnore
    val allPredicates: Map<UUID, Predicate> by lazy {
        knowledgeBases
            .flatMap { it.modules }
            .flatMap { it.predicates }
            .associateBy { it.uuid }
    }

    @get:JsonIgnore
    val allIndices: Map<UUID, Index> by lazy {
        allPredicates.values
            .flatMap { it.indices }
            .associateBy { it.uuid }
    }

    init {
        setBackwardsReferences()
    }

    fun withModifiedPredicate(uuid: UUID, modifier: (Predicate) -> Predicate): SystemCatalog {
        check(uuid in allPredicates) { "A predicate with UUID $uuid does not exist" }
        return copy(
            knowledgeBases = knowledgeBases
                .map { kb -> KnowledgeBase(
                    name = kb.name,
                    defaultModule = kb.defaultModule,
                    modules = kb.modules.map { module ->
                        Module(
                            module.name,
                            module.predicates.map { predicate ->
                                if (predicate.uuid == uuid) modifier(predicate) else predicate
                            }.toSet(),
                            module.prologSource
                        )
                    }.toSet()
                )}
                .toSet()
        )
    }

    fun withModifiedIndex(uuid: UUID, modifier: (Index) -> Index): SystemCatalog {
        val indexBefore = allIndices[uuid]
            ?: throw IllegalStateException("An index with UUID $uuid does not exist")
        return withModifiedPredicate(indexBefore.predicate.uuid) { predicate ->
            predicate.copy(
                indices = predicate.indices.map { index ->
                    if (index.uuid == predicate.uuid) modifier(index) else index
                }.toSet()
            )
        }
    }

    // TODO: identify by uuid
    data class KnowledgeBase(
        val name: String,
        val modules: Set<Module>,
        val defaultModule: String?
    ) {
        @get:JsonIgnore
        val modulesByName: Map<String, Module> by lazy {
            modules.associateBy { it.name }
        }

        @get:JsonIgnore
        val allPredicatesByFqi: Map<FullyQualifiedClauseIndicator, Predicate> by lazy {
            modules
                .flatMap { it.predicates }
                .associateBy { it.indicator }
        }
    }

    // TODO: identify by uuid
    data class Module(
        val name: String,
        val predicates: Set<Predicate>,
        val prologSource: String
    ) {
        @get:JsonIgnore
        lateinit var knowledgeBase: KnowledgeBase
            internal set
    }

    data class Predicate(
        override val functor: String,
        override val arity: Int,
        val uuid: UUID,
        /**
         * @see com.github.prologdb.storage.fact.FactStoreImplementationLoader.implementationId
         */
        val factStoreImplementationId: String?,
        val indices: Set<Index> = emptySet(),
    ) : HasFunctorAndArity {
        @get:JsonIgnore
        lateinit var module: Module
            internal set

        @get:JsonIgnore
        val indicator: FullyQualifiedClauseIndicator by lazy {
            FullyQualifiedClauseIndicator(module.name, ClauseIndicator.of(this))
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Predicate

            if (uuid != other.uuid) return false

            return true
        }

        override fun hashCode(): Int {
            return uuid.hashCode()
        }
    }

    data class Index(
        val name: String,
        val uuid: UUID,
        val unscopedTemplateGoal: Query,
        val key: Set<Variable>,
        val storeAdditionally: Set<Variable>,
        val factIndexImplementationId: String?,
    ) {
        @get:JsonIgnore
        lateinit var predicate: Predicate
            internal set

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Index

            if (uuid != other.uuid) return false

            return true
        }

        override fun hashCode(): Int {
            return uuid.hashCode()
        }
    }

    fun nextRevisionNumber(): Long {
        var new = revision + 1
        if (new < 0) {
            new = 1
        }
        return new
    }

    private fun setBackwardsReferences() {
        knowledgeBases.forEach { kb ->
            kb.modules.forEach { module ->
                module.knowledgeBase = kb
                module.predicates.forEach { predicate ->
                    predicate.module = module
                    predicate.indices.forEach { index ->
                        index.predicate = predicate
                    }
                }
            }
        }
    }

    companion object {
        val INITIAL: SystemCatalog = SystemCatalog(0, emptySet())
    }
}