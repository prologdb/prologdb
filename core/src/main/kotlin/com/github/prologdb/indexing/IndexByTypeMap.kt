package com.github.prologdb.indexing

import com.github.prologdb.indexing.index.IndexFeature
import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.runtime.term.Term
import kotlin.reflect.KClass

/**
 * For a particular [PredicateIndicator] and argument index, describes
 * the [PredicateArgumentIndex] available for that argument, by type
 * of the indexed argument.
 */
interface IndexByTypeMap {
    val prototype: PredicateIndicator

    val argumentIndex: Int

    /**
     * The [PredicateArgumentIndex] suitable for the argument or `null` if the
     * given type of [Term] is not indexed at this argument.
     * @throws IndexingException If `type == Term::class`
     */
    operator fun get(type: KClass<out Term>): PredicateArgumentIndex?

    /**
     * Creates a new [PredicateArgumentIndex] for the given type at the respective argument index.
     * @param features The features the index should have; the best suitable implementation will be chosen
     * @throws IllegalArgumentException if `type == Term::class`
     * @throws IllegalStateException if `get(type) != null` by the time this method is invoked
     */
    fun createForType(type: KClass<out Term>, vararg features: IndexFeature)

    /** @return True when there are no indexes defined, false otherwise */
    fun isEmpty(): Boolean
}