package com.github.prologdb.storage.fact

import com.github.prologdb.dbms.DataDirectoryManager
import kotlin.reflect.KClass

/**
 * Creates anew / loads from persistence [FactStore]s of only one particular type.
 * There should be one implementation of this interface for each implementation of
 * [FactStore].
 */
interface SpecializedFactStoreLoader<T : FactStore> {

    /**
     * The type of [FactStore] created by this factory.
     */
    val type: KClass<T>

    /**
     * Create a new [FactStore] of type [T] or load an existing fact store from
     * persistence.
     * @throws StorageException
     */
    fun createOrLoad(directoryManager: DataDirectoryManager.PredicateScope): T

    /**
     * Assures that any files associated with the fact store for given predicate
     * are removed from the filesystem and the disk space occupied is made available.
     */
    fun destroy(directoryManager: DataDirectoryManager.PredicateScope)
}