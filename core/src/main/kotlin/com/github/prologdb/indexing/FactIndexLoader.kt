package com.github.prologdb.indexing

import com.github.prologdb.dbms.DataDirectoryManager

/**
 * Creates [FactIndex]s for given [IndexDefinition]s. Each instance of [PersistentKnowledgeBase]
 * has exactly one instance of this type; it is responsible for managing all indices in that
 * knowledge base.
 * 
 * Implementations need not be thread-safe. Users should synchronize on the loader instance.
 */
interface FactIndexLoader {
    /**
     * Creates a new index with the given name:
     * * sets up and allocates storage files
     * * saves metadata about the index so that it can, even after a restart, be discovered through
     *   [open].
     * @param directoryManager The location and scope of the index
     */
    fun create(directoryManager: DataDirectoryManager.ClauseStoreScope, definition: IndexDefinition): FactIndex

    /**
     * Opens and returns the index with the given [IndexDefinition.name]. The other properties
     * of [IndexDefinition] are ignored.
     * 
     * @param directoryManager The location and scope of the index
     *
     * @throws IndexNotFoundException If an index with the given name does not exist.
     */
    @Throws(IndexNotFoundException::class)
    fun open(directoryManager: DataDirectoryManager.ClauseStoreScope, definition: IndexDefinition): FactIndex
}
