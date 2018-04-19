package com.github.prologdb.indexing

class ArrayIndexSet(val indexes: IntArray) : IndexSet {
    constructor(list: Collection<Int>) : this(list.toIntArray())
    constructor(indexSet: IndexSet): this(indexSet.toList())

    override fun iterator(): IntIterator = indexes.iterator()

    override fun union(other: IndexSet): IndexSet {
        val otherAsListIndexSet = ArrayIndexSet(other)
        return ArrayIndexSet(indexes.union(otherAsListIndexSet))
    }
}