package com.github.prologdb.indexing

import com.github.prologdb.runtime.ArityMap
import com.github.prologdb.runtime.knowledge.library.*
import com.github.prologdb.runtime.term.Predicate

/** Maps functor names to `T`s */
private typealias FunctorMap<T> = MutableMap<String,T>

class IndexedLibraryEntryStore : MutableLibraryEntryStore {
    private val indexedItems: FunctorMap<ArityMap<IndexedPartialLibraryEntryStore>> = HashMap()

    override val exports: Iterable<LibraryEntry>
        get() = indexedItems.values.flatMap { it.values() }.flatMap(IndexedPartialLibraryEntryStore::exports)

    override fun add(entry: LibraryEntry) {
        getPartialStoreFor(entry).add(entry)
    }

    override fun findFor(predicate: Predicate): Iterable<LibraryEntry> {
        val partialStore = getPartialStoreFor(predicate)

        var indexSet: IndexSet? = null
        for (argumentIndex in 0 until predicate.arity) {
            val index = partialStore.getIndexForArgument(argumentIndex)
            if (index != null) {
                val argumentValue = predicate.arguments[argumentIndex]
                if (indexSet == null) {
                    indexSet = index.find(argumentValue)
                } else {
                    indexSet = indexSet.union(index.find(argumentValue))
                }
            }
        }

        indexSet ?: TODO("Fall back to table scan")

        return object : Iterable<LibraryEntry> {
            override fun iterator(): Iterator<LibraryEntry> {
                val indexIterator = indexSet.iterator()
                return object : Iterator<LibraryEntry> {
                    override fun hasNext(): Boolean = indexIterator.hasNext()
                    override fun next(): LibraryEntry = partialStore.exports[indexIterator.next()]
                }
            }
        }
    }

    private fun getPartialStoreFor(prototype: PredicatePrototype): IndexedPartialLibraryEntryStore {
        var arityMap = indexedItems[prototype.name]
        if (arityMap == null) {
            arityMap = ArityMap()
            indexedItems[prototype.name] = arityMap
        }

        var partialStore = arityMap[prototype.arity]
        if (partialStore == null) {
            partialStore = TODO()
            arityMap[prototype.arity] = partialStore
        }

        return partialStore
    }
}