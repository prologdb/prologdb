package com.github.prologdb.storage.fact

import com.github.prologdb.dbms.DataDirectoryManager
import com.github.prologdb.storage.StorageException
import com.github.prologdb.util.metadata.load
import com.github.prologdb.util.metadata.set

private val META_KEY = "fact_store_impl"

/**
 * The default implementation to [FactStoreLoader].
 *
 * Desired features in [create] are weighed like follows:
 *
 * 1. Every desired feature is assigned a weight: the number of desired features minus its iteration index
 * 2. For every known implementation, a score is calculated: the sum of the weight of the features it has
 * 3. The implementation with the highest score is selected
 * 4. In case of a score-wise draw, the implementation with more desired, distinct feature support wins:
 *     suppose the desired features are F1, F2, F3 and we have two implementations with the maximum score:
 *     A1 supporting F2 and F3 and A2 supporting only F1. A2 wins in this case because F1 is most desired
 *     and distinct to A2.
 * 5. If there is still a draw (e.g. for multiple implementations with equal featuresets),
 *    it is not defined which implementation is selected.
 */
open class DefaultFactStoreLoader : FactStoreLoader {

    private val knownSpecializedLoaders: MutableSet<SpecializedFactStoreLoader<*>> = mutableSetOf()

    /**
     * Adds the given [SpecializedFactStoreLoader] to the known loaders. The loader will be
     * considered for calls to [create] and [load].
     */
    fun registerSpecializedLoader(loader: SpecializedFactStoreLoader<*>) {
        synchronized(knownSpecializedLoaders) {
            if (loader !in knownSpecializedLoaders) {
                if (knownSpecializedLoaders.any { it.type == loader.type }) {
                    throw IllegalStateException("A specialized loader for fact store type ${loader.type} is already registered.")
                }

                knownSpecializedLoaders.add(loader)
            }
        }
    }

    override fun create(directoryManager: DataDirectoryManager.ClauseStoreScope, requiredFeatures: Set<FactStoreFeature>, desiredFeatures: Set<FactStoreFeature>): FactStore {
        val indicator = directoryManager.indicator
        var implClassName = directoryManager.metadata.load<String>(META_KEY)

        if (implClassName != null) {
            throw StorageException("A fact store for $indicator already exists.")
        }

        val loader = selectImplementation(requiredFeatures, desiredFeatures)
        implClassName = loader.type.qualifiedName ?: throw StorageException("Qualified class name of fact store implementation ${loader.type} cannot be determined.")

        val store = loader.createOrLoad(directoryManager)
        directoryManager.metadata[META_KEY] = implClassName

        return store
    }

    override fun load(directoryManager: DataDirectoryManager.ClauseStoreScope): FactStore? {
        val indicator = directoryManager.indicator
        val implClassName = directoryManager.metadata.load<String>(META_KEY) ?: return null
        val loader = try {
            getLoader(implClassName)
        }
        catch (ex: Throwable) {
            throw StorageException("Failed to obtain loader for fact store of $indicator", ex)
        }

        return loader.createOrLoad(directoryManager)
    }

    /**
     * **MUST BE THREAD-SAFE!**
     * @return the loader for the given class name
     */
    protected fun getLoader(implClassName: String): SpecializedFactStoreLoader<*> {
        synchronized(knownSpecializedLoaders) {
            return knownSpecializedLoaders
                .firstOrNull { it.type.qualifiedName == implClassName }
                ?: throw StorageException("Specialized loader for store implementation $implClassName is not registered.")
        }
    }

    protected fun selectImplementation(requiredFeatures: Set<FactStoreFeature>, desiredFeatures: Set<FactStoreFeature>): SpecializedFactStoreLoader<*> {
        val implsFittingRequirements = knownSpecializedLoaders.filter { loader ->
            requiredFeatures.all { feature -> feature.isSupportedBy(loader.type) }
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

        val desiredFeaturesWithWeight: Set<Pair<FactStoreFeature, Int>> = desiredFeatures
            .mapIndexed { index, loader -> Pair(loader, desiredFeatures.size - index) }
            .toSet()

        val implementationsWithScore: Set<Pair<SpecializedFactStoreLoader<*>, Int>> = implsFittingRequirements
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
        var drawImpls: List<SpecializedFactStoreLoader<*>> = implementationsWithTopScore
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