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
     */
    fun load(
        dbName: String,
        forPredicatesOf: PredicateIndicator
    ) : PredicateStore
}

/**
 * The default implementation to [PredicateStoreLoader] with the following implementation
 * for the desired features weighing algorithm:
 *
 * 1. Every desired feature is assigned its index in the iterated set (most desired to least index)
 * 2. For every known implementation, a score is calculated: the sum of the indices of the features it has
 * 3. The implementation with the lowest score is selected
 * 4. In case of a score-wise draw, the implementation with more supported more desired features is selected
 * 5. If there is still a draw (e.g. for multiple implementations with equal featuresets),
 *    it is not defined which implementation is selected.
 */
class DefaultPredicateStoreFactory : PredicateStoreLoader {

    private val knownSpecializedLoaders: MutableSet<SpecializedPredicateStoreLoader<*>> = mutableSetOf()

    /**
     * Adds the given [SpecializedPredicateStoreLoader] to the known loaders. The loader will be
     * considered for calls to [create] and [load].
     */
    fun registerSpecializedLoader(loader: SpecializedPredicateStoreLoader<*>) {
        knownSpecializedLoaders.add(loader)
    }

    override fun create(dbName: String, forPredicatesOf: PredicateIndicator, requiredFeatures: Set<PredicateStoreFeature>, desiredFeatures: Set<PredicateStoreFeature>): PredicateStore {
        if (storeExists(dbName, forPredicatesOf)) {
            throw StorageException("A predicate store for $forPredicatesOf already exists in database $dbName")
        }

        val loader = selectImplementation(requiredFeatures, desiredFeatures)
        return loader.createOrLoad(dbName, forPredicatesOf)
    }

    override fun load(dbName: String, forPredicatesOf: PredicateIndicator): PredicateStore {
        val loader: SpecializedPredicateStoreLoader<*> = TODO("find loader for dbName and forPredicatesOf")
        return loader.createOrLoad(dbName, forPredicatesOf)
    }

    private fun storeExists(dbName: String, forPredicatesOf: PredicateIndicator): Boolean {
        TODO()
    }

    private fun selectImplementation(requiredFeatures: Set<PredicateStoreFeature>, desiredFeatures: Set<PredicateStoreFeature>): SpecializedPredicateStoreLoader<*> {
        val implsFittingRequirements = knownSpecializedLoaders.filter { loader ->
            desiredFeatures.all { feature -> feature.isSupportedBy(loader.type) }
        }

        if (implsFittingRequirements.isEmpty()) {
            val requirementsNotFulfilled = requiredFeatures.filter { feature ->
                knownSpecializedLoaders.none { loader -> feature.isSupportedBy(loader.type) }
            }

            if (requirementsNotFulfilled.isEmpty()) {
                throw StorageException("The combination of the required features $requiredFeatures is not fulfilled by any known implementation.")
            } else {
                throw StorageException("These required features are not supported by any known implementation: $requirementsNotFulfilled")
            }
        }

        val desiredFeaturesWithScore: Set<Pair<PredicateStoreFeature, Int>> = desiredFeatures
            .mapIndexed { index, loader -> Pair(loader, desiredFeatures.size - index) }
            .toSet()

        val implementationsWithScore: Set<Pair<SpecializedPredicateStoreLoader<*>, Int>> = implsFittingRequirements
            .map { loader ->
                val score = desiredFeaturesWithScore
                    .map { (feature, featureScore) ->
                        if (feature.isSupportedBy(loader.type)) featureScore else 0
                    }
                    .sum()

                Pair(loader, score)
            }
            .toSet()

        TODO()
    }
}