package com.github.prologdb.dbms

import com.github.prologdb.execplan.planner.ExecutionPlanner
import com.github.prologdb.execplan.planner.NoOptimizationExecutionPlanner
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.storage.fact.DefaultFactStoreLoader
import com.github.prologdb.storage.fact.FactStore
import com.github.prologdb.storage.fact.FactStoreLoader
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class PrologDatabase(
    dataDirectory: Path,
    private val factStoreLoader: FactStoreLoader = DefaultFactStoreLoader(),
    val executionPlanner: ExecutionPlanner = NoOptimizationExecutionPlanner()
) {
    val dataDirectory = DataDirectoryManager.open(dataDirectory)

    private val factStores: MutableMap<UUID, FactStore> = ConcurrentHashMap()
    private val factStoreLoadingMutex = Any()

    private val runtimeEnvironmentByCatalogRevision: MutableMap<Long, MutableMap<String, DatabaseRuntimeEnvironment>> = ConcurrentHashMap()

    fun createKnowledgeBase(name: String) {
        dataDirectory.modifySystemCatalog { catalog ->
            if (catalog.knowledgeBases.any { it.name == name }) {
                throw PrologRuntimeException("A knowledge base with the name $name already exists.")
            }

            return@modifySystemCatalog catalog.copy(knowledgeBases = catalog.knowledgeBases + SystemCatalog.KnowledgeBase(
                name,
                emptySet()
            ))
        }
    }

    fun getRuntimeEnvironment(systemCatalog: SystemCatalog, knowledgeBaseName: String): DatabaseRuntimeEnvironment {
        val knowledgeBaseCatalog = systemCatalog.knowledgeBases.firstOrNull { it.name == knowledgeBaseName }
            ?: throw PrologRuntimeException("Knowledge base $knowledgeBaseName does not exist.")

        return runtimeEnvironmentByCatalogRevision
            .computeIfAbsent(systemCatalog.revision) { ConcurrentHashMap() }
            .computeIfAbsent(knowledgeBaseCatalog.name) {
                DatabaseRuntimeEnvironment(knowledgeBaseCatalog, this)
            }
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