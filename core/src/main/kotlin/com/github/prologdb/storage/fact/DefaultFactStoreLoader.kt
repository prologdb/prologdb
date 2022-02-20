package com.github.prologdb.storage.fact

import com.github.prologdb.dbms.DataDirectoryManager
import com.github.prologdb.storage.StorageException
import org.slf4j.LoggerFactory
import java.util.ServiceLoader
import java.util.concurrent.ConcurrentHashMap

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

    private val knownSpecializedLoadersById: MutableMap<String, FactStoreImplementationLoader> = ConcurrentHashMap()

    /**
     * Adds the given [FactStoreImplementationLoader] to the known loaders. The loader will be
     * considered for calls to [create] and [load].
     */
    fun registerSpecializedLoader(loader: FactStoreImplementationLoader) {
        if (knownSpecializedLoadersById.putIfAbsent(loader.implementationId, loader) == null) {
            throw DuplicateFactStoreImplementationException(loader.implementationId)
        }
    }

    override fun create(directoryManager: DataDirectoryManager.PredicateScope, requiredFeatures: Set<FactStoreFeature>, desiredFeatures: Set<FactStoreFeature>): FactStore {
        val loader = selectImplementation(requiredFeatures, desiredFeatures)
        return create(directoryManager, loader)
    }

    override fun create(
        directoryManager: DataDirectoryManager.PredicateScope,
        implementationId: String
    ): FactStore {
        val loader = knownSpecializedLoadersById[implementationId]
            ?: throw FactStoreImplementationUnknownException(implementationId)
        return create(directoryManager, loader)
    }

    private fun create(
        directoryManager: DataDirectoryManager.PredicateScope,
        loader: FactStoreImplementationLoader
    ): FactStore {
        lateinit var factStore: FactStore
        directoryManager.modifyPredicateCatalog { catalog ->
            if (catalog.factStoreImplementationId != null) {
                throw StorageException("A fact store for predicate ${directoryManager.uuid} already exists.")
            }

            factStore = loader.createOrLoad(directoryManager)
            catalog.copy(factStoreImplementationId = loader.implementationId)
        }

        return factStore
    }

    override fun load(directoryManager: DataDirectoryManager.PredicateScope): FactStore? {
        return getLoader(directoryManager)?.createOrLoad(directoryManager)
    }

    override fun destroy(directoryManager: DataDirectoryManager.PredicateScope) {
        getLoader(directoryManager)?.destroy(directoryManager)
    }

    protected fun getLoader(directoryManager: DataDirectoryManager.PredicateScope): FactStoreImplementationLoader? {
        val implClassName = directoryManager.catalogEntry.factStoreImplementationId ?: return null
        return getLoader(implClassName)
    }

    /**
     * **MUST BE THREAD-SAFE!**
     * @return the loader for the given class name
     */
    protected fun getLoader(implementationId: String): FactStoreImplementationLoader {
        synchronized(knownSpecializedLoadersById) {
            return knownSpecializedLoadersById[implementationId]
                ?: throw StorageException("Specialized loader for store implementation $implementationId is not registered.")
        }
    }

    protected fun selectImplementation(requiredFeatures: Set<FactStoreFeature>, desiredFeatures: Set<FactStoreFeature>): FactStoreImplementationLoader {
        val allImplsLocal = knownSpecializedLoadersById.values.toList()
        val implsFittingRequirements = allImplsLocal.filter { loader ->
            requiredFeatures.all(loader::supportsFeature)
        }

        if (implsFittingRequirements.isEmpty()) {
            val requirementsNotFulfilled = requiredFeatures.filter { feature ->
                allImplsLocal.none { loader -> loader.supportsFeature(feature) }
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

        val implementationsWithScore: Set<Pair<FactStoreImplementationLoader, Int>> = implsFittingRequirements
            .map { loader ->
                val score = desiredFeaturesWithWeight.sumOf { (feature, weight) ->
                    if (loader.supportsFeature(feature)) weight else 0
                }

                Pair(loader, score)
            }
            .toSet()

        val topScore = implementationsWithScore.maxOfOrNull { it.second }
        val implementationsWithTopScore = implementationsWithScore.filter { it.second == topScore }.map { it.first }

        if (implementationsWithTopScore.size == 1) {
            return implementationsWithTopScore.first()
        }

        // there is a draw; go through the features by weight decreasing
        // if a non-empty subset of the implementations supports the feature,
        // reduce the working set to those and go on to the next feature
        var drawImpls: List<FactStoreImplementationLoader> = implementationsWithTopScore
        for (desiredFeature in desiredFeatures) {
            val implsSupportingFeature = drawImpls.filter { it.supportsFeature(desiredFeature) }
            if (implsSupportingFeature.isNotEmpty()) {
                drawImpls = implsSupportingFeature
            }
        }

        // if there is still more than one implementation left, it is
        // not predictable which one ist the first in the list. Hence
        // the contract defines that it is not defined which implementation will be used.
        return drawImpls.first()
    }

    companion object {
        private val log = LoggerFactory.getLogger("prologdb.storage")
        @JvmStatic
        fun withServiceLoaderImplementations() : DefaultFactStoreLoader {
            val loader = DefaultFactStoreLoader()
            ServiceLoader.load(FactStoreImplementationLoader::class.java).forEach { impl ->
                try {
                    loader.registerSpecializedLoader(impl)
                }
                catch (ex: DuplicateFactStoreImplementationException) {
                    log.warn("Ignoring fact store implementation ${impl::class.qualifiedName} because another class already provides an implementation for ${ex.id}")
                }
            }

            return loader
        }
    }
}