package com.github.prologdb.indexing

import com.github.prologdb.indexing.index.IndexFeature
import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.runtime.term.Term
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

/**
 * For a particular [PredicateIndicator] and argument index, describes
 * the [PredicateArgumentIndex] available for that argument, by type
 * of the indexed argument.
 */
interface IndexByTypeMap {
    val indicator: PredicateIndicator

    val argumentIndex: Int

    /**
     * The [PredicateArgumentIndex] suitable for the argument or `null` if the
     * given type of [Term] is not indexed at this argument.
     * @throws IndexingException If `type == Term::class`
     */
    operator fun get(type: KClass<out Term>, vararg desiredFeatures: IndexFeature): PredicateArgumentIndex?

    /**
     * Creates a new [PredicateArgumentIndex] for the given type at the respective argument index.
     * @param features The features the index should have; the best suitable implementation will be chosen. Features
     * listed earlier will get higher priority
     * @throws IllegalArgumentException if `type == Term::class`
     * @throws IllegalStateException if `get(type) != null` by the time this method is invoked
     */
    fun createForType(type: KClass<out Term>, vararg features: IndexFeature)

    /** @return True when there are no indexes defined, false otherwise */
    fun isEmpty(): Boolean
}

internal class DefaultIndeyByTypeMap(
    override val indicator: PredicateIndicator,
    override val argumentIndex: Int,
    private val factories: Set<PredicateArgumentIndexFactory<out PredicateArgumentIndex>>
) : IndexByTypeMap {
    private val indices = mutableSetOf<IndexRegistration>()

    fun registerExistingIndex(index: PredicateArgumentIndex, forValueType: KClass<out Term>) {
        indices.add(IndexRegistration(index, forValueType))
    }

    override fun get(type: KClass<out Term>, vararg desiredFeatures: IndexFeature): PredicateArgumentIndex? {
        if (type == Term::class) throw IllegalArgumentException("Type must not be Term::class")

        return indices
            .filter { it.argumentType.isSubclassOf(type) }
            .sortedByDescending { indexRegistration ->
                var score = 0
                desiredFeatures.forEachIndexed { invertedFeaturePriority, feature ->
                    if (feature isSupportedBy indexRegistration.index) {
                        score += desiredFeatures.size - invertedFeaturePriority
                    }
                }
                score
            }
            .firstOrNull()?.index
    }

    override fun createForType(type: KClass<out Term>, vararg features: IndexFeature) {
        val factory = factories
            .sortedByDescending { factory ->
                var score = 0
                features.forEachIndexed { invertedFeaturePriority, feature ->
                    if (feature isSupportedBy factory.type) {
                        score += features.size - invertedFeaturePriority
                    }
                }
                score
            }
            .firstOrNull()

        factory ?: throw IndexingException("Cannot create index with the given features: no suitable factory registered")

        indices.add(IndexRegistration(factory.create(type), type))
    }

    override fun isEmpty(): Boolean = indices.isEmpty()

    private data class IndexRegistration(
        val index: PredicateArgumentIndex,
        val argumentType: KClass<out Term>
    )
}