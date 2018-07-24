package com.github.prologdb

import com.github.prologdb.indexing.PersistenceIDSet
import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.runtime.term.Predicate

/**
 * Stores instances of a particular [PredicateIndicator] in a 0-index based random-access manner.
 *
 * This is pretty much the equivalent to an SQL table (or as close as it gets).
 */
interface PredicateStore {
    /** The type of predicate stored in this object */
    val indicator: PredicateIndicator

    /** @return all indexes that are valid to query from [getByIndex] */
    fun all(): PersistenceIDSet

    /** @return the predicate instance stored at the given index or `null` if that space is currently unoccupied. */
    fun getByIndex(index: Int): Predicate?
}