package com.github.prologdb.storage.predicate

import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
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
     * @param dbName The name of the database for which to create the predicate store.
     * @param forPredicates The indicator that will apply to all predicates stored in
     *                      the created [PredicateStore].
     * @throws StorageException
     */
    fun createOrLoad(dbName: String, forPredicates: ClauseIndicator): T
}