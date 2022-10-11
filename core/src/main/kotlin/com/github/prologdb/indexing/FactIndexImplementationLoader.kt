package com.github.prologdb.indexing

import com.github.prologdb.dbms.DataDirectoryManager

interface FactIndexImplementationLoader {
    /**
     * Identifies this implementation. This string is written to the data directory so that the database
     * can construct the correct implementation for the data on disc. Thus, this string must remain constant
     * at all time.
     */
    val implementationId: String

    /**
     * Create a new [FactIndex] of type [T] or load an existing fact index from
     * persistence.
     * @throws StorageException
     */
    fun createOrLoad(directoryManager: DataDirectoryManager.IndexScope): FactIndex

    /**
     * Assures that any files associated with the fact index
     * are removed from the filesystem and the disk space occupied is made available.
     */
    fun destroy(directoryManager: DataDirectoryManager.IndexScope)

    /**
     * Whether this fact store implementation supports the given feature.
     */
    fun supportsFeature(feature: FactIndexFeature): Boolean
}