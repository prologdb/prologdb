package com.github.prologdb.storage.fact

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.Principal
import com.github.prologdb.runtime.term.Term
import java.util.concurrent.Future

/**
 * Stores all facts for a [SystemCatalog.Predicate].
 * Implementations MUST be thread-safe.
 */
interface FactStore {
    /**
     * Stores the given predicate in the storage and returns the associated
     * ID.
     * @throws OutOfStorageSpaceException
     */
    fun store(asPrincipal: Principal, arguments: Array<out Term>): Future<PersistenceID>

    /**
     * @return The fact that was [store]d with the given
     * [PersistenceID]; null if no such fact was ever stored or when
     * it was [delete]d
     */
    fun retrieve(asPrincipal: Principal, id: PersistenceID): Future<Array<out Term>?>

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
    fun all(asPrincipal: Principal): LazySequence<Pair<PersistenceID, Array<out Term>>>

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