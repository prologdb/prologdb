package com.github.prologdb.indexing

import com.github.prologdb.runtime.ArityMap
import com.github.prologdb.runtime.knowledge.library.*
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification

class IndexedLibraryEntryStore(
    private val indexFactory: IndexedPartialLibraryEntryStoreFactory
) : MutableLibraryEntryStore {
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

        if (indexSet != null) {
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

        // index is null; this means that the partialStore has no indexes defined.
        // no index means full table scan
        return partialStore.exports
    }

    private fun getPartialStoreFor(prototype: PredicatePrototype): IndexedPartialLibraryEntryStore {
        var arityMap = indexedItems[prototype.name]
        if (arityMap == null) {
            arityMap = ArityMap()
            indexedItems[prototype.name] = arityMap
        }

        var partialStore = arityMap[prototype.arity]
        if (partialStore == null) {
            partialStore = indexFactory.createEntryStoreFor(prototype)
            arityMap[prototype.arity] = partialStore
        }

        return partialStore
    }

    override fun retract(unifiesWith: Predicate): LazySequence<Unification> {
        TODO()
    }

    override fun retractFact(fact: Predicate): LazySequence<Unification> {
        TODO()
    }
}

/** Maps functor names to `T`s */
private typealias FunctorMap<T> = MutableMap<String,T>