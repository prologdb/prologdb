package com.github.prologdb.storage.predicate

import com.github.prologdb.util.metadata.MetadataRepository
import com.github.prologdb.util.metadata.get
import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.util.metadata.set
import com.github.prologdb.storage.StorageException
import kotlin.reflect.KClass

private val METADATA_REPOSITORY_KEY = "predicate_stores"

/**
 * The default implementation to [PredicateStoreLoader] with the following implementation
 * for the desired features weighing algorithm:
 *
 * 1. Every desired feature is assigned a weight: the number of desired features minus its iteration index
 * 2. For every known implementation, a score is calculated: the sum of the weight of the features it has
 * 3. The implementation with the highest score is selected
 * 4. In case of a score-wise draw, the implementation of those with the highest score with more supported
 *     more desired features is selected: say desired features are F1, F2, F3 and we have two implementations
 *     with the maximum score: A1 supporting F2 and F3 and A2 supporting only F1. A2 wins in this case.
 * 5. If there is still a draw (e.g. for multiple implementations with equal featuresets),
 *    it is not defined which implementation is selected.
 */
class DefaultPredicateStoreLoader(
    val metadataRepository: MetadataRepository
) : PredicateStoreLoader {

    private val knownSpecializedLoaders: MutableSet<SpecializedPredicateStoreLoader<*>> = mutableSetOf()

    /**
     * The metadata; initialized on object creation time. When changes are made they must be flushed
     * manually by calling [MetadataRepository.save] on [metadataRepository]
     */
    private var metadataMirror: DefaultPredicateStoreFactoryMetadata = metadataRepository[METADATA_REPOSITORY_KEY]
        ?: DefaultPredicateStoreFactoryMetadata(mutableSetOf())

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
        val store = loader.createOrLoad(dbName, forPredicatesOf)
        metadataMirror.stores += ExistingPredicateStore(
            dbName,
            forPredicatesOf.name,
            forPredicatesOf.arity,
            loader.type.qualifiedName ?: store::class.qualifiedName!!
        )
        metadataRepository[METADATA_REPOSITORY_KEY] = metadataMirror

        return store
    }

    override fun load(dbName: String, forPredicatesOf: PredicateIndicator): PredicateStore? {
        val implementationClassName = findStoreImplementationClassName(dbName, forPredicatesOf) ?: return null
        val loader = knownSpecializedLoaders
            .firstOrNull { it.type.qualifiedName == implementationClassName }
            ?: throw StorageException("Cannot load predicate store for $forPredicatesOf: specialized loader for store implementation $implementationClassName is not registered.")

        return loader.createOrLoad(dbName, forPredicatesOf)
    }

    private fun storeExists(dbName: String, forPredicatesOf: PredicateIndicator): Boolean {
        return findStoreImplementationClassName(dbName, forPredicatesOf) != null
    }

    /**
     * @return The [KClass.qualifiedName] of the implementation for the given store or `null` if no
     * store exists for the given database and predicate.
     * @see ExistingPredicateStore.implementationClassName
     */
    private fun findStoreImplementationClassName(dbName: String, forPredicatesOf: PredicateIndicator): String? {
        return metadataMirror.stores
            .firstOrNull {
                it.predicateArity == forPredicatesOf.arity && it.dbName == dbName && it.predicateName == forPredicatesOf.name
            }
            ?.implementationClassName
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

        val desiredFeaturesWithWeight: Set<Pair<PredicateStoreFeature, Int>> = desiredFeatures
            .mapIndexed { index, loader -> Pair(loader, desiredFeatures.size - index) }
            .toSet()

        val implementationsWithScore: Set<Pair<SpecializedPredicateStoreLoader<*>, Int>> = implsFittingRequirements
            .map { loader ->
                val score = desiredFeaturesWithWeight
                    .map { (feature, weight) ->
                        if (feature.isSupportedBy(loader.type)) weight else 0
                    }
                    .sum()

                Pair(loader, score)
            }
            .toSet()

        val topScore = implementationsWithScore.map { it.second }.max()
        val implementationsWithTopScore = implementationsWithScore.filter { it.second == topScore }.map { it.first }

        if (implementationsWithTopScore.size == 1) {
            return implementationsWithTopScore.first()
        }

        // there is a draw; go through the features by weight decreasing
        // if a non-empty subset of the implementations supports the feature,
        // reduce the working set to those and go on to the next feature
        var drawImpls: List<SpecializedPredicateStoreLoader<*>> = implementationsWithTopScore
        for (desiredFeature in desiredFeatures) {
            val implsSupportingFeature = drawImpls.filter { desiredFeature.isSupportedBy(it.type) }
            if (implsSupportingFeature.isNotEmpty()) {
                drawImpls = implsSupportingFeature
            }
        }

        // if there is still more than one implementation left, it is
        // not predictable which one ist the first in the list. Hence
        // the contract defines that it is not defined which implementation will be used.
        return drawImpls.first()
    }
}

/**
 * Models the JSON for persistence of the information which predicate
 * stores exist and what implementation was chosen.
 */
private data class DefaultPredicateStoreFactoryMetadata(
    val stores: MutableSet<ExistingPredicateStore>
)

private data class ExistingPredicateStore(
    val dbName: String,
    val predicateName: String,
    val predicateArity: Int,
    /**
     * The [KClass.qualifiedName] (package and class) of the
     * predicate store used. To be corelated with [SpecializedPredicateStoreLoader.type]
     * (rather than [PredicateStore.javaClass]).
     */
    val implementationClassName: String
)