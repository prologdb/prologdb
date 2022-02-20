package com.github.prologdb.storage.fact

import com.github.prologdb.dbms.DataDirectoryManager

/**
 * Creates a new / loads from persistence [FactStore]s of only one particular type.
 * There should be one implementation of this interface for each implementation of
 * [FactStore].
 */
interface FactStoreImplementationLoader {

    /**
     * Identifies this implementation. This string is written to the data directory so that the database
     * can construct the correct implementation for the data on disc. Thus, this string must remain constant
     * at all time.
     */
    val implementationId: String

    /**
     * Create a new [FactStore] of type [T] or load an existing fact store from
     * persistence.
     * @throws StorageException
     */
    fun createOrLoad(directoryManager: DataDirectoryManager.PredicateScope): FactStore

    /**
     * Assures that any files associated with the fact store for given predicate
     * are removed from the filesystem and the disk space occupied is made available.
     */
    fun destroy(directoryManager: DataDirectoryManager.PredicateScope)

    /**
     * Whether this fact store implementation supports the given feature.
     */
    fun supportsFeature(feature: FactStoreFeature): Boolean
}