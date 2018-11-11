package com.github.prologdb.indexing

import com.github.prologdb.runtime.term.Term
import com.github.prologdb.storage.predicate.PersistenceID

/**
 * Indexes an argument of a predicate, e.g. indexes the 2nd argument to `likes/2`. Given any term, finds the indexes
 * of all facts and/or rules within the table where the second argument is likely to unify with the given term.
 */
interface PredicateArgumentIndex {
    /**
     * If the same table index (`atIndex`) is inserted multiple
     * times without being removed in between insertions the behaviour of the entire index (all methods) is undefined.
     *
     * @return the indexes in the source list at which the entry's argument is likely to unify with
     *         the given term
     */
    fun find(argument: Term): PersistenceIDSet

    /**
     * To be called when a predicate is inserted into the underlying list. Updates the index accordingly. Other rows
     * in the table and index are not affected by this operation.
     *
     * If the same table index (`atIndex`) is inserted multiple
     * times without being removed in between insertions the behaviour of the entire index (all methods) is undefined.
     *
     * @param argumentValue The value of the argument in the added predicate
     * @param atPersistenceID Index in the source list of predicates where the new predicate is being inserted to.
     */
    fun onInserted(argumentValue: Term, atPersistenceID: PersistenceID)

    /**
     * To be called when a predicate is removed from the underlying list. Updates the index accordingly. Other rows
     * in the table and index are not affected by this operation.
     *
     * If the same table index (`atIndex`) is inserted multiple
     * times without being removed in between insertions the behaviour of the entire index (all methods) is undefined.
     *
     * @param argumentValue The value of the argument in the removed predicate
     * @param fromPersistenceID Index in the source list of predicates from which the term is being removed.
     */
    fun onRemoved(argumentValue: Term, fromPersistenceID: PersistenceID)
}

/**
 * A [PredicateArgumentIndex] that supports efficient range-queries (faster than O(n) where n is the number of rows
 * over which to query).
 */
interface RangeQueryPredicateArgumentIndex : PredicateArgumentIndex {
    /**
     * @param lowerBound the lower bound. If null, there is no lower bound (query for lessThan / lessThanOrEqualTo)
     * @param upperBound the upper bound. If null, there is no upper bound (query for greaterThan / greaterThanOrEqualTo)
     * @return the indexes in the source list at which the entry's argument is between lowerBound and upper bound
     * @throws IllegalArgumentException If both `lowerBound` and `upperBound` are `null`
     */
    fun findBetween(lowerBound: Term?, lowerInclusive: Boolean, upperBound: Term?, upperInclusive: Boolean): PersistenceIDSet
}