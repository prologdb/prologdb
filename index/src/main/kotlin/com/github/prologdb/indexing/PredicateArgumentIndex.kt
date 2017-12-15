package com.github.prologdb.indexing

import com.github.prologdb.runtime.term.Term

/**
 * Indexes an argument of a predicate, e.g. indexes the 2nd argument to `likes/2`. Given any term, finds the indexes
 * of all facts and/or rules within the list the index was created from, where the second argument is likely to
 * unify with the given term.
 */
interface PredicateArgumentIndex {
    /**
     * @return the indexes in the source list at which the entry's argument is likely to unify with
     *         the given term
     */
    fun find(argument: Term): IndexSet

    /**
     * To be called when a predicate is inserted into the underlying list. Updates the index accordingly.
     * @param argumentValue The value of the argument in the added predicate
     * @param atIndex Index in the source list of predicates where the new predicate is being inserted to. All entries
     *                after the given index will move right by one (increment index by 1) to make room for the to-be-inserted
     *                entry.
     */
    fun onInserted(argumentValue: Term, atIndex: Int)

    /**
     * To be called when a predicate is removed from the underlying list. Updates the index accordingly.
     * @param argumentValue The value of the argument in the removed predicate
     * @param fromIndex Index in the source list of predicates from which the term is being removed. All entries
     *                  after the given index will move left by one (decrement index by 1) to close the gap that
     *                  removing the entry left.
     */
    fun onRemoved(argumentValue: Term, fromIndex: Int)
}