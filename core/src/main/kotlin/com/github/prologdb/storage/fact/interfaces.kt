package com.github.prologdb.storage.fact

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.Principal
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.term.CompoundTerm
import java.util.concurrent.Future

/**
 * An identifier for individual facts stored in a [FactStore].
 * One such indentifier is only meaningful to the [FactStore]
 * that defined it.
 */
typealias PersistenceID = Long

/**
 * Stores all predicates that match a given [ClauseIndicator].
 * Implementations MUST be thread-safe.
 */
interface FactStore {
    /** Indicates the predicate for which facts are stored in this store. */
    val indicator: ClauseIndicator

    /**
     * Stores the given predicate in the storage and returns the associated
     * ID.
     * @throws OutOfStorageSpaceException
     */
    fun store(asPrincipal: Principal, item: CompoundTerm): Future<PersistenceID>

    /**
     * @return The fact that was [store]d with the given
     * [PersistenceID]; null if no such fact was ever stored or when
     * it was [delete]d
     */
    fun retrieve(asPrincipal: Principal, id: PersistenceID): Future<CompoundTerm?>

    /**
     * Removes the fact stored with the given ID from
     * the store. Subsequent calls to [retrieve] with the same
     * [PersistenceID] will return null.
     *
     * @return Whether the given [PersistenceID] was associated with a
     * fact.
     */
    fun delete(asPrincipal: Principal, id: PersistenceID): Future<Boolean>

    /**
     * @return A lazy sequence of all the facts stored in this store.
     */
    fun all(asPrincipal: Principal): LazySequence<Pair<PersistenceID, CompoundTerm>>

    /**
     * Does these things in sequence:
     * * Causes any new operations to fail with an error indicating the the
     *   fact store is closed.
     * * Waits for all ongoing operations to complete
     * * Flushes all pending changes to disk
     * * Frees as many handles and resources as possible
     */
    fun close()
}

/**
 * Thrown for all persistence related errors / exceptions, including IO
 */
open class PrologPersistenceException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * Thrown when data cannot be stored because storage space is depleated.
 */
class OutOfStorageSpaceException(message: String, cause: Throwable? = null) : PrologPersistenceException(message, cause)