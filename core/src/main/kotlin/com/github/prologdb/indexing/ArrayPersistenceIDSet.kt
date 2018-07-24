package com.github.prologdb.indexing

import com.github.prologdb.storage.predicate.PersistenceID

/**
 * Implements [PersistenceIDSet] based on a simple given array of [PersistenceID]s.
 */
class ArrayPersistenceIDSet(val ids: Array<PersistenceID>) : PersistenceIDSet {
    constructor(list: Collection<PersistenceID>) : this(list.toTypedArray())
    constructor(persistenceIDSet: PersistenceIDSet): this(persistenceIDSet.toList())

    override fun iterator(): Iterator<PersistenceID> = ids.iterator()

    override fun union(other: PersistenceIDSet): PersistenceIDSet {
        val otherAsListIndexSet = ArrayPersistenceIDSet(other)
        return ArrayPersistenceIDSet(ids.union(otherAsListIndexSet))
    }
}