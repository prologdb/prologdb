package com.github.prologdb.indexing

import com.github.prologdb.runtime.term.Term
import kotlin.reflect.KClass

/**
 * Creates [PredicateArgumentIndex] of exactly one type/class.
 */
interface PredicateArgumentIndexFactory<ProducedType : PredicateArgumentIndex> {
    val type: KClass<ProducedType>

    /**
     * Creates a new index for values of the given type.
     */
    fun create(forTermType: KClass<out Term>): ProducedType
}