package com.github.prologdb.storage.predicate

import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.storage.StorageException

/**
 * Creates new [PredicateStore]s for given [PredicateIndicator]s.
 *
 * Implementations need not be thread safe. Code using objects of this
 * type should synchronize on the loader object.
 */
interface PredicateStoreLoader {
    /**
     * Creates a new [PredicateStore] for predicates of the given indicator.
     * @param dbName The name of the database for which to create the new store.
     * @param requiredFeatures The returned predicate store is guaranteed to have all these features
     *                         contained in [requiredFeatures].
     * @param desiredFeatures The factory will try to find an implementation that has the desired features
     *                        on a best-effort/best-match basis. The actual algorithm is implementation-defined.
     *                        Implementations may interpret the natural order of this set to be the order of
     *                        descending priority (e.g. first element in the set is the most desired feature).
     *                        Sets created using [setOf] expose that behaviour of natural order.
     * @throws ConflictingFeaturesException If two or more of the features in [requiredFeatures] are in conflict with
     *                                      each other
     * @throws StorageException If a predicate store for the given database and predicate indicator already exists.
     */
    fun create(
        dbName: String,
        forPredicatesOf: PredicateIndicator,
        requiredFeatures: Set<PredicateStoreFeature>,
        desiredFeatures: Set<PredicateStoreFeature>
    ) : PredicateStore

    /**
     * Loads the [PredicateStore] for predicates of the given indicator. Though stores can be loaded
     * multiple times. If both loaded stores are used, the behaviour of the stores is undefined.
     * @param dbName The name of the database for which to load the store.
     * @return the loaded store or `null` if a store for the given database and predicate does not exist yet.
     */
    fun load(
        dbName: String,
        forPredicatesOf: PredicateIndicator
    ) : PredicateStore?
}