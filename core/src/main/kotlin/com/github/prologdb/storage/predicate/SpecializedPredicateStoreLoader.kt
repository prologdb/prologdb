package com.github.prologdb.storage.predicate

import com.github.prologdb.dbms.DataDirectoryManager
import kotlin.reflect.KClass

/**
 * Creates anew / loads from persistence [PredicateStore]s of only one particular type.
 * There should be one implementation of this interface for each implementation of
 * [PredicateStore].
 */
interface SpecializedPredicateStoreLoader<T : PredicateStore> {

    /**
     * The type of [PredicateStore] created by this factory.
     */
    val type: KClass<T>

    /**
     * Create a new [PredicateStore] of type [T] or load an existing predicate store from
     * persistence.
     * @param forPredicates The indicator that will apply to all predicates stored in
     *                      the created [PredicateStore].
     * @throws StorageException
     */
    fun createOrLoad(directoryManager: DataDirectoryManager.ClauseStoreScope): T
}