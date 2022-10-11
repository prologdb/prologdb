package com.github.prologdb.indexing

import com.github.prologdb.dbms.DataDirectoryManager

/**
 * Creates [FactIndex]s for given [IndexDefinition]s. Each instance of [PhysicalKnowledgeBaseRuntimeEnvironment]
 * has exactly one instance of this type; it is responsible for managing all indices in that
 * knowledge base.
 * 
 * Implementations need not be thread-safe. Users should synchronize on the loader instance.
 */
interface FactIndexLoader {

    fun create(
        directoryManager: DataDirectoryManager.IndexScope,
        requiredFeatures: Set<FactIndexFeature>,
        desiredFeatures: Set<FactIndexFeature>
    ): FactIndex

    fun create(
        directoryManager: DataDirectoryManager.IndexScope,
        implementationId: String,
    ): FactIndex

    fun load(directoryManager: DataDirectoryManager.IndexScope): FactIndex?

    fun destroy(directoryManager: DataDirectoryManager.IndexScope)
}
