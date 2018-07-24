package com.github.prologdb.indexing

import com.github.prologdb.runtime.knowledge.library.PredicateIndicator

/**
 * A description of indices available for a particular kind of predicate
 * (see [indicator])
 */
interface IndexByArgumentMap {
    val indicator: PredicateIndicator

    /**
     * The available [PredicateArgumentIndex]es available for the given argument index, by
     * type of the indexed argument.
     * @throws IllegalArgumentException if `argumentIndex < 0`
     */
    operator fun get(argumentIndex: Int): IndexByTypeMap
}