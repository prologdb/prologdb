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
}