package com.github.prologdb.storage.predicate

import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.term.Predicate

/**
 * An identifier for individual predicates stored in a [PredicateStore].
 * One such indentifier is only meaningful to the [PredicateStore]
 * that defined it.
 */
typealias PersistenceID = Long

/**
 * Stores all predicates that match a given [PredicateIndicator].
 * Implementations MUST be thread-safe.
 */
interface PredicateStore {
    /** Indicates the kinds of predicates stored in this store. */
    val indicator: PredicateIndicator

    /**
     * Stores the given predicate in the storage and returns the associated
     * ID.
     * @throws OutOfStorageSpaceException
     */
    fun store(item: Predicate): PersistenceID

    /**
     * @return The predicate that was [store]d with the given
     * [PersistenceID]; null if no such predicate was ever stored or when
     * it was [delete]d
     */
    fun retrieve(id: PersistenceID): Predicate?

    /**
     * Removes the predicate stored with the given ID from
     * the store. Subsequent calls to [retrieve] with the same
     * [PersistenceID] will return null.
     *
     * @return Whether the given [PersistenceID] was assoicated with a
     * predicate.
     */
    fun delete(id: PersistenceID): Boolean

    /**
     * @return A lazy sequence of all the predicates stored in this store.
     */
    fun all(): LazySequence<Pair<PersistenceID, Predicate>>
}

/**
 * Thrown for all persistence related errors / exceptions, including IO
 */
open class PrologPersistenceException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * Thrown when data cannot be stored because storage space is depleated.
 */
class OutOfStorageSpaceException(message: String, cause: Throwable? = null) : PrologPersistenceException(message, cause)