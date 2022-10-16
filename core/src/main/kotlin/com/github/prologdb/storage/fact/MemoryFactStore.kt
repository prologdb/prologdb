package com.github.prologdb.storage.fact

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.Principal
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.dbms.DataDirectoryManager
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.ImplFeature
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future

/**
 * A [FactStore] that works only in memory.
 */
class MemoryFactStore(val arity: Int) : FactStore {

    /** Stores facts. Gets resized throughout the lifetime */
    private var store = Array<Array<out Term>?>(100) { null }

    /** To be [synchronized] on when mutating [store] */
    private val storeMutationMutex = Any()

    /**
     * When running out of space in [store], [store] will be enlarged by
     * this factor
     */
    private val upscalingFactor = 1.5

    override fun store(asPrincipal: Principal, arguments: Array<out Term>): Future<PersistenceID> {
        if (arguments.size != arity) {
            throw IllegalArgumentException("This store holds only predicates of arity $arity, got ${arguments.size} arguments")
        }

        synchronized(storeMutationMutex) {
            for (index in store.indices) {
                if (store[index] == null) {
                    store[index] = arguments
                    return CompletableFuture<PersistenceID>().apply { complete(index.toLong()) }
                }
            }

            // store is too small, enlarge
            val newStore = Array(Math.floor(store.size.toDouble() * upscalingFactor).toInt(), {
                i -> if (i <= store.lastIndex) store[i] else null
            })

            newStore[store.size] = arguments
            val id = store.size.toLong()
            store = newStore

            return CompletableFuture<PersistenceID>().apply { complete(id) }
        }
    }

    override fun retrieve(asPrincipal: Principal, id: PersistenceID): Future<Array<out Term>?> {
        val idAsInt = id.toInt()
        if (idAsInt.toLong() != id) {
            throw IllegalArgumentException()
        }

        val future = CompletableFuture<Array<out Term>?>()

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
                return CompletableFuture<Boolean>().apply { complete(false) }
            }

            val wasThere = store[idAsInt] != null
            store[idAsInt] = null

            return CompletableFuture<Boolean>().apply { complete(wasThere) }
        }
    }

    override fun all(asPrincipal: Principal): LazySequence<Pair<PersistenceID, Array<out Term>>> {
        val _store = store
        return buildLazySequence(asPrincipal) {
            yieldAllFinal(
                _store.asSequence()
                    .filterNotNull()
                    .mapIndexed { persistenceIdAsInt, fact ->
                        Pair(persistenceIdAsInt.toLong(), fact)
                    }
            )
        }
    }

    override fun close() {
        // nothing to do, really
    }

    class Loader : FactStoreImplementationLoader {

        override val implementationId = "storage.facts.firstparty.in-memory"

        override fun createOrLoad(directoryManager: DataDirectoryManager.PredicateScope): FactStore {
            return MemoryFactStore(directoryManager.catalogEntry.indicator.indicator.arity)
        }

        override fun destroy(directoryManager: DataDirectoryManager.PredicateScope) {

        }

        override fun supportsFeature(feature: ImplFeature): Boolean {
            return feature in setOf(ImplFeature.ACCELERATED)
        }
    }
}
