package com.github.prologdb.storage.fact

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.Principal
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.storage.AcceleratedStorage
import com.github.prologdb.storage.VolatileStorage
import com.sun.xml.internal.ws.util.CompletedFuture
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future

/**
 * A [FactStore] that works only in memory.
 */
@VolatileStorage
@AcceleratedStorage
class MemoryFactStore(override val indicator: ClauseIndicator) : FactStore {

    /** Stores facts. Gets resized throughout the lifetime */
    private var store = Array<CompoundTerm?>(100) { null }

    /** To be [synchronized] on when mutating [store] */
    private val storeMutationMutex = Any()

    /**
     * When running out of space in [store], [store] will be enlarged by
     * this factor
     */
    private val upscalingFactor = 1.5

    override fun store(asPrincipal: Principal, item: CompoundTerm): Future<PersistenceID> {
        if (item.arity != indicator.arity || item.functor != indicator.name) {
            throw IllegalArgumentException("This store holds only predicates of type ${indicator.name}/${indicator.arity}")
        }

        synchronized(storeMutationMutex) {
            for (index in store.indices) {
                if (store[index] == null) {
                    store[index] = item
                    return CompletedFuture(index.toLong(), null)
                }
            }

            // store is too small, enlarge
            val newStore = Array<CompoundTerm?>(Math.floor(store.size.toDouble() * upscalingFactor).toInt(), {
                i -> if (i <= store.lastIndex) store[i] else null
            })

            newStore[store.size] = item
            val id = store.size.toLong()
            store = newStore

            return CompletedFuture(id, null)
        }
    }

    override fun retrieve(asPrincipal: Principal, id: PersistenceID): Future<CompoundTerm?> {
        val idAsInt = id.toInt()
        if (idAsInt.toLong() != id) {
            throw IllegalArgumentException()
        }

        val future = CompletableFuture<CompoundTerm?>()

        val _store = store
        if (idAsInt > _store.lastIndex) {
            future.complete(null)
        } else {
            future.complete(_store[idAsInt])
        }
        return future
    }

    override fun delete(asPrincipal: Principal, id: PersistenceID): Future<Boolean> {
        val idAsInt = id.toInt()
        if (idAsInt.toLong() != id) {
            throw IllegalArgumentException()
        }

        synchronized(storeMutationMutex) {
            if (idAsInt > store.lastIndex) {
                return CompletedFuture(false, null)
            }

            val wasThere = store[idAsInt] != null
            store[idAsInt] = null

            return CompletedFuture(wasThere, null)
        }
    }

    override fun all(asPrincipal: Principal): LazySequence<Pair<PersistenceID, CompoundTerm>> {
        val _store = store
        return buildLazySequence(asPrincipal) {
            _store.forEachIndexed { persistenceIDAsInt, predicate ->
                if (predicate == null) return@forEachIndexed
                yield(Pair(persistenceIDAsInt.toLong(), predicate))
            }
        }
    }

    override fun close() {
        // nothing to to, really
    }
}