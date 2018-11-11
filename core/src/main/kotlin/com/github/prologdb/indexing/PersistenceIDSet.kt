package com.github.prologdb.indexing

import com.github.prologdb.storage.predicate.PersistenceID

/**
 * An immutable set of [PersistenceID]s relating to a [PredicateStore].
 */
interface PersistenceIDSet : Iterable<PersistenceID> {
    /**
     * @return an iterator over all indexes in the set
     */
    override fun iterator(): Iterator<PersistenceID>

    /**
     * @return the union set of this [PersistenceIDSet] and the given [PersistenceIDSet]
     */
    infix fun union(other: PersistenceIDSet): PersistenceIDSet

    companion object {
        /**
         * An empty [PersistenceIDSet]
         */
        val NONE: PersistenceIDSet = object : PersistenceIDSet {
            override fun iterator(): Iterator<PersistenceID> = emptyList<PersistenceID>().iterator()
            override fun union(other: PersistenceIDSet): PersistenceIDSet = this
        }
    }
}