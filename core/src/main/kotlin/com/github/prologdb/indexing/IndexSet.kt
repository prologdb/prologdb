package com.github.prologdb.indexing

/**
 * Set of indices related to a list. This is its own class to reduce (un)boxing overhead and allow for lazy
 * computation+traversal of union sets.
 */
interface IndexSet : Iterable<Int> {
    /**
     * @return an iterator over all indexes in the set
     */
    override fun iterator(): IntIterator

    /**
     * @return the union set of this [IndexSet] and the given [IndexSet]
     */
    infix fun union(other: IndexSet): IndexSet

    companion object {
        /**
         * An empty [IndexSet]
         */
        val NONE: IndexSet = object : IndexSet {
            override fun iterator(): IntIterator = IntRange.EMPTY.iterator()
            override fun union(other: IndexSet): IndexSet = this
        }
    }
}