package com.github.prologdb.dbms

import com.github.prologdb.execplan.planner.ExecutionPlanner
import com.github.prologdb.execplan.planner.NoOptimizationExecutionPlanner
import com.github.prologdb.runtime.PrologRuntimeEnvironment
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.module.ModuleScopeProofSearchContext
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.storage.fact.DefaultFactStoreLoader
import com.github.prologdb.storage.fact.FactStore
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

    private val runtimeEnvironmentByCatalogRevision: MutableMap<Long, MutableMap<String, PhysicalDatabaseRuntimeEnvironment>> = ConcurrentHashMap()

    init {
        log.info("Starting in $dataDirectory with system catalog revision ${this.dataDirectory.systemCatalog.revision}")
    }

    fun createKnowledgeBase(name: String) {
        dataDirectory.modifySystemCatalog { catalog ->
            if (catalog.knowledgeBases.any { it.name == name }) {
                throw PrologRuntimeException("A knowledge base with the name $name already exists.")
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
                throw PrologRuntimeException("A knowledge base with the name $newName already exists.")
            }

            return@modifySystemCatalog catalog.copy(knowledgeBases = (catalog.knowledgeBases - knowledgeBaseCatalog) + knowledgeBaseCatalog.copy(
                name = newName
            ))
        }
    }

    fun dropKnowledgeBase(name: String) {
        if (name == SystemCatalog.META_KNOWLEDGE_BASE_NAME) {
            throw PrologRuntimeException("Cannot delete the meta knowledge base.")
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

    fun getRuntimeEnvironment(systemCatalog: SystemCatalog, knowledgeBaseSpecifier: Term): PrologRuntimeEnvironment {
        if (knowledgeBaseSpecifier is Atom) {
            val knowledgeBaseName = knowledgeBaseSpecifier.name

            val knowledgeBaseCatalog = systemCatalog.knowledgeBases.firstOrNull { it.name == knowledgeBaseName }
                ?: throw KnowledgeBaseNotFoundException(knowledgeBaseName)

            return runtimeEnvironmentByCatalogRevision
                .computeIfAbsent(systemCatalog.revision) { ConcurrentHashMap() }
                .computeIfAbsent(knowledgeBaseCatalog.name) {
                    PhysicalDatabaseRuntimeEnvironment(knowledgeBaseCatalog, this)
                }
        }

        if (knowledgeBaseSpecifier is CompoundTerm && knowledgeBaseSpecifier.functor == "meta" && knowledgeBaseSpecifier.arity == 1) {
            val nameTerm = knowledgeBaseSpecifier.arguments[0]
            if (nameTerm is Atom) {
                val knowledgeBaseName = nameTerm.name
                val knowledgeBaseCatalog = systemCatalog.knowledgeBases.firstOrNull { it.name == knowledgeBaseName }
                    ?: throw KnowledgeBaseNotFoundException(knowledgeBaseName)

                return MetaKnowledgeBaseRuntimeEnvironment(knowledgeBaseCatalog)
            }
        }

        throw KnowledgeBaseNotFoundException(knowledgeBaseSpecifier)
    }

    fun getFactStore(predicateUuid: UUID): FactStore {
        factStores[predicateUuid]?.let { return it }
        synchronized(factStoreLoadingMutex) {
            factStores[predicateUuid]?.let { return it }
            val factStore = factStoreLoader.load(dataDirectory.scopedForPredicate(predicateUuid))
                ?: throw PrologRuntimeException("There is no fact store for Predicate $predicateUuid yet.")
            factStores[predicateUuid] = factStore
            return factStore
        }
    }

    fun close() {
        dataDirectory.close()
    }
}