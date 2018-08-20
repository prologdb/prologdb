package com.github.prologdb.storage.predicate

import com.github.prologdb.runtime.knowledge.library.PredicateIndicator

/**
 * Creates new [PredicateStore]s for given [PredicateIndicator]s.
 */
interface PredicateStoreFactory {
    /**
     * Creates a new [PredicateStore] for predicates of the given indicator.
     * @param requiredFeatures The returned predicate store is guaranteed to have all these features
     *                         contained in [requiredFeatures].
     * @param desiredFeatures The factory will try to find an implementation that has the desired features
     *                        on a best-effort/best-match basis. The actual algorithm is implementation-defined.
     *                        Implementations may interpret the natural order of this set to be the order of
     *                        descending priority (e.g. first element in the set is the most desired feature).
     *                        Sets created using [setOf] expose that behaviour of natural order.
     * @throws ConflictingFeaturesException If two or more of the features in [requiredFeatures] are in conflict with
     *                                      each other
     * @throws
     */
    fun create(
        forPredicatesOf: PredicateIndicator,
        requiredFeatures: Set<PredicateStoreFeature>,
        desiredFeatures: Set<PredicateStoreFeature>
    ): PredicateStore
}