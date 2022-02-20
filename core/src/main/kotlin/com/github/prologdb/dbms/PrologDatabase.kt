package com.github.prologdb.dbms

import com.github.prologdb.execplan.planner.ExecutionPlanner
import com.github.prologdb.execplan.planner.NoOptimizationExecutionPlanner
import com.github.prologdb.runtime.PrologUnsupportedOperationException
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.storage.MissingFactStoreException
import com.github.prologdb.storage.fact.DefaultFactStoreLoader
import com.github.prologdb.storage.fact.FactStore
import com.github.prologdb.storage.fact.FactStoreFeature
import com.github.prologdb.storage.fact.FactStoreLoader
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

private val log = LoggerFactory.getLogger("prologdb.master")

class PrologDatabase(
    dataDirectory: Path,
    private val factStoreLoader: FactStoreLoader = DefaultFactStoreLoader(),
    val executionPlanner: ExecutionPlanner = NoOptimizationExecutionPlanner()
) {
    val dataDirectory = DataDirectoryManager.open(dataDirectory)

    private val factStores: MutableMap<UUID, FactStore> = ConcurrentHashMap()
    private val factStoreLoadingMutex = Any()

    private val runtimeEnvironmentByCatalogRevision: MutableMap<Long, MutableMap<String, DefaultPhysicalKnowledgeBaseRuntimeEnvironment>> = ConcurrentHashMap()

    private val globalMetaRuntimeEnvironment by lazy { GlobalMetaKnowledgeBaseRuntimeEnvironment(this) }

    init {
        log.info("Starting in $dataDirectory with system catalog revision ${this.dataDirectory.systemCatalog.revision}")
    }

    fun createKnowledgeBase(name: String) {
        dataDirectory.modifySystemCatalog { catalog ->
            if (catalog.knowledgeBases.any { it.name == name }) {
                throw KnowledgeBaseAlreadyExistsException(Atom(name))
            }

            return@modifySystemCatalog catalog.copy(knowledgeBases = catalog.knowledgeBases + SystemCatalog.KnowledgeBase(
                name,
                emptySet(),
                null
            ))
        }
    }

    fun renameKnowledgeBase(oldName: String, newName: String) {
        dataDirectory.modifySystemCatalog { catalog ->
            val knowledgeBaseCatalog = catalog.knowledgeBases.firstOrNull { it.name == oldName }
                ?: throw KnowledgeBaseNotFoundException(oldName)

            if (catalog.knowledgeBases.any { it.name == newName }) {
                throw KnowledgeBaseAlreadyExistsException(Atom(newName))
            }

            return@modifySystemCatalog catalog.copy(knowledgeBases = (catalog.knowledgeBases - knowledgeBaseCatalog) + knowledgeBaseCatalog.copy(
                name = newName
            ))
        }
    }

    fun dropKnowledgeBase(name: String) {
        if (name == GlobalMetaKnowledgeBaseRuntimeEnvironment.KNOWLEDGE_BASE_NAME) {
            throw PrologUnsupportedOperationException("Cannot delete the meta knowledge base.")
        }
        // TODO: instead, mark as deleted with TXID

        lateinit var knowledgeBaseCatalog: SystemCatalog.KnowledgeBase
        dataDirectory.modifySystemCatalog { catalog ->
             knowledgeBaseCatalog = catalog.knowledgeBases.firstOrNull { it.name == name }
                ?: throw KnowledgeBaseNotFoundException(name)

            return@modifySystemCatalog catalog.copy(knowledgeBases = catalog.knowledgeBases - knowledgeBaseCatalog)
        }

        knowledgeBaseCatalog.allPredicatesByFqi.values.forEach {
            factStoreLoader.destroy(dataDirectory.scopedForPredicate(it.uuid))
        }
    }

    fun getRuntimeEnvironment(systemCatalog: SystemCatalog, knowledgeBaseSpecifier: Term): DatabaseRuntimeEnvironment {
        if (knowledgeBaseSpecifier is Atom) {
            val knowledgeBaseName = knowledgeBaseSpecifier.name

            if (knowledgeBaseName == GlobalMetaKnowledgeBaseRuntimeEnvironment.KNOWLEDGE_BASE_NAME) {
                return globalMetaRuntimeEnvironment
            }

            val knowledgeBaseCatalog = systemCatalog.knowledgeBases.firstOrNull { it.name == knowledgeBaseName }
                ?: throw KnowledgeBaseNotFoundException(knowledgeBaseName)

            return runtimeEnvironmentByCatalogRevision
                .computeIfAbsent(systemCatalog.revision) { ConcurrentHashMap() }
                .computeIfAbsent(knowledgeBaseCatalog.name) {
                    DefaultPhysicalKnowledgeBaseRuntimeEnvironment(knowledgeBaseCatalog, this)
                }
        }

        if (knowledgeBaseSpecifier is CompoundTerm && knowledgeBaseSpecifier.functor == MetaKnowledgeBaseRuntimeEnvironment.KNOWLEDGE_BASE_SPECIFIER_FUNCTOR && knowledgeBaseSpecifier.arity == 1) {
            val nameTerm = knowledgeBaseSpecifier.arguments[0]
            if (nameTerm is Atom) {
                val knowledgeBaseName = nameTerm.name
                val knowledgeBaseCatalog = systemCatalog.knowledgeBases.firstOrNull { it.name == knowledgeBaseName }
                    ?: throw KnowledgeBaseNotFoundException(knowledgeBaseName)

                return MetaKnowledgeBaseRuntimeEnvironment(this, knowledgeBaseCatalog)
            }
        }

        throw KnowledgeBaseNotFoundException(knowledgeBaseSpecifier)
    }

    @Throws(MissingFactStoreException::class)
    fun getFactStore(predicateUuid: UUID): FactStore {
        factStores[predicateUuid]?.let { return it }
        synchronized(factStoreLoadingMutex) {
            factStores[predicateUuid]?.let { return it }
            val factStore = factStoreLoader.load(dataDirectory.scopedForPredicate(predicateUuid))
                ?: throw MissingFactStoreException(predicateUuid)
            factStores[predicateUuid] = factStore
            return factStore
        }
    }

    fun createFactStore(predicateUuid: UUID, implementationId: String): FactStore {
        synchronized(factStoreLoadingMutex) {
            return factStoreLoader.create(dataDirectory.scopedForPredicate(predicateUuid), implementationId)
        }
    }

    fun createFactStore(predicateUuid: UUID, requiredFeatures: Set<FactStoreFeature>, desiredFeatures: Set<FactStoreFeature>): FactStore {
        synchronized(factStoreLoadingMutex) {
            return factStoreLoader.create(dataDirectory.scopedForPredicate(predicateUuid), requiredFeatures, desiredFeatures)
        }
    }

    fun close() {
        dataDirectory.close()
    }
}