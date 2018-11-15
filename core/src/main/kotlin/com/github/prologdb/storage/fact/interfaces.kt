package com.github.prologdb.storage.fact

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.Principal
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.Predicate
import java.util.concurrent.Future

/**
 * An identifier for individual predicates stored in a [FactStore].
 * One such indentifier is only meaningful to the [FactStore]
 * that defined it.
 */
typealias PersistenceID = Long

/**
 * Stores all predicates that match a given [ClauseIndicator].
 * Implementations MUST be thread-safe.
 */
interface FactStore {
    /** Indicates the kinds of predicates stored in this store. */
    val indicator: ClauseIndicator

    /**
     * Stores the given predicate in the storage and returns the associated
     * ID.
     * @throws OutOfStorageSpaceException
     */
    fun store(asPrincipal: Principal, item: Predicate): Future<PersistenceID>

    /**
     * @return The predicate that was [store]d with the given
     * [PersistenceID]; null if no such predicate was ever stored or when
     * it was [delete]d
     */
    fun retrieve(asPrincipal: Principal, id: PersistenceID): Future<Predicate?>

    /**
     * Removes the predicate stored with the given ID from
     * the store. Subsequent calls to [retrieve] with the same
     * [PersistenceID] will return null.
     *
     * @return Whether the given [PersistenceID] was assoicated with a
     * predicate.
     */
    fun delete(asPrincipal: Principal, id: PersistenceID): Future<Boolean>

    /**
     * @return A lazy sequence of all the predicates stored in this store.
     */
    fun all(asPrincipal: Principal): LazySequence<Pair<PersistenceID, Predicate>>
}

/**
 * Thrown for all persistence related errors / exceptions, including IO
 */
open class PrologPersistenceException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * Thrown when data cannot be stored because storage space is depleated.
 */
class OutOfStorageSpaceException(message: String, cause: Throwable? = null) : PrologPersistenceException(message, cause)