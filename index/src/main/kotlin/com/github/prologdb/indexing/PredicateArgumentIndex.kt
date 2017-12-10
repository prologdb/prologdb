package com.github.prologdb.indexing

import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.term.Term

/**
 * Indexes an argument of a predicate, e.g. indexes the 2nd argument to `likes/2`. Given any term, finds the indexes
 * of all facts and/or rules within the list the index was created from, where the second argument is likely to
 * unify with the given term.
 */
interface PredicateArgumentIndex {
    /**
     * The predicate functor this index applies to
     */
    val functor: String

    /**
     * The argument index being indexed by this index
     */
    val argumentIndex: Int

    /**
     * @return the indexes in the source list at which the entry's [argumentIndex]th argument is likely to unify with
     *         the given term
     */
    fun find(argument: Term): IndexSet

    /**
     * To be called when a predicate is added to the underlying list. Updates the index accordingly.
     */
    fun onAdded(predicate: Predicate, atIndex: Int)

    /**
     * To be called when a predicate is removed from the underlying list. Updates the index accordingly.
     */
    fun onRemoved(predicate: Predicate, fromIndex: Int)
}