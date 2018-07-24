package com.github.prologdb.storage.predicate

import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.lazysequence.buildLazySequence
import com.github.prologdb.runtime.term.Predicate

/**
 * A [PredicateStore] that works only in memory.
 */
class MemoryPredicateStore(override val indicator: PredicateIndicator) : PredicateStore {

    /** Stores predicates gets resized throughout the lifetime */
    private var store = Array<Predicate?>(100, { null })

    /** To be [synchronized] on when mutating [store] and/or [nPredicates] */
    private val storeMutationMutex = Any()

    /**
     * When running out of space in [store], [store] will be enlarged by
     * this factor
     */
    private val upscalingFactor = 1.5

    override fun store(item: Predicate): PersistenceID {
        if (item.arity != indicator.arity || item.name != indicator.name) {
            throw IllegalArgumentException("This store holds only predicates of type ${indicator.name}/${indicator.arity}")
        }

        synchronized(storeMutationMutex) {
            for (index in store.indices) {
                if (store[index] == null) {
                    store[index] = item
                    return index.toLong()
                }
            }

            // store is too small, enlarge
            val newStore = Array<Predicate?>(Math.floor(store.size.toDouble() * upscalingFactor).toInt(), {
                i -> if (i <= store.lastIndex) store[i] else null
            })

            newStore[store.size] = item
            val id = store.size.toLong()
            store = newStore

            return id
        }
    }

    override fun retrieve(id: PersistenceID): Predicate? {
        val idAsInt = id.toInt()
        if (idAsInt.toLong() != id) {
            throw IllegalArgumentException()
        }

        val _store = store
        if (idAsInt > _store.lastIndex) return null
        return _store[idAsInt]
    }

    override fun delete(id: PersistenceID): Boolean {
        val idAsInt = id.toInt()
        if (idAsInt.toLong() != id) {
            throw IllegalArgumentException()
        }

        synchronized(storeMutationMutex) {
            if (idAsInt > store.lastIndex) {
                return false
            }

            val wasThere = store[idAsInt] != null
            store[idAsInt] = null

            return wasThere
        }
    }

    override fun all(): LazySequence<Pair<PersistenceID, Predicate>> {
        val _store = store
        return buildLazySequence {
            _store.forEachIndexed { persistenceIDAsInt, predicate ->
                if (predicate == null) return@forEachIndexed
                yield(Pair(persistenceIDAsInt.toLong(), predicate))
            }
        }
    }
}