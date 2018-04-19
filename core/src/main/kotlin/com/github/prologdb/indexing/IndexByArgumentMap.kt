package com.github.prologdb.indexing

import com.github.prologdb.runtime.knowledge.library.PredicatePrototype

/**
 * A description of indexed available for a particular [IndexedPartialPredicateStore]
 */
interface IndexByArgumentMap {
    val prototype: PredicatePrototype

    /**
     * The available [PredicateArgumentIndex]es available for the given argument index, by
     * type of the indexed argument.
     * @throws IllegalArgumentException if `argumentIndex < 0`
     */
    operator fun get(argumentIndex: Int): IndexByTypeMap
}