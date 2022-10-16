package com.github.prologdb.storage.index

import com.github.prologdb.ImplFeature
import com.github.prologdb.async.*
import com.github.prologdb.dbms.DataDirectoryManager
import com.github.prologdb.indexing.*
import com.github.prologdb.storage.fact.PersistenceID
import com.github.prologdb.util.concurrency.locks.RegionReadWriteLockManager
import java.util.TreeMap
import java.util.concurrent.atomic.AtomicBoolean

class MemoryIndex(
    private val indexDirectory: DataDirectoryManager.IndexScope
) : FactIndex, RangeQueryFactIndex {
    private val data = TreeMap<IndexKey, MutableList<IndexEntry>>(Comparator<IndexKey> { a, b ->
        indexDirectory.indexDefinition.keyComparator.compare(a.value, b.value)
    })
    private val dataLock = RegionReadWriteLockManager("in-memory-index.${indexDirectory.uuid}")
        .get(0..0)

    override val definition: IndexDefinition = indexDirectory.indexDefinition

    override fun find(key: IndexKey, principal: Principal): LazySequence<IndexEntry> {
        return buildLazySequence(principal) {
            await(dataLock.readLock.acquireFor(principal))
            try {
                return@buildLazySequence yieldAllFinal(
                    data[key]?.asSequence() ?: emptySequence()
                )
            }
            finally {
                dataLock.readLock.releaseFor(principal)
            }
        }
    }

    override fun onInserted(entry: IndexEntry, principal: Principal): WorkableFuture<Unit> {
        val key = indexDirectory.indexDefinition.getKey(entry)
        return launchWorkableFuture(principal) {
            awaitAndFinally(dataLock.writeLock.acquireFor(principal)) {
                dataLock.writeLock.releaseFor(principal)
            }

            data.computeIfAbsent(key, {_ -> ArrayList()}).add(entry)
        }
    }

    override fun onRemoved(persistenceID: PersistenceID, key: IndexKey, principal: Principal): WorkableFuture<Unit> {
        return launchWorkableFuture(principal) {
            awaitAndFinally(dataLock.writeLock.acquireFor(principal)) {
                dataLock.writeLock.releaseFor(principal)
            }

            data.computeIfPresent(key) { _, entries ->
                entries.removeIf { it.persistenceId == persistenceID }
                entries
            }
        }
    }

    override fun findBetween(
        lowerBound: IndexKey?,
        lowerInclusive: Boolean,
        upperBound: IndexKey?,
        upperInclusive: Boolean,
        principal: Principal
    ): LazySequence<IndexEntry> {
        require(lowerBound != null || upperBound != null)

        return buildLazySequence(principal) {
            await(dataLock.readLock.acquireFor(principal))

            val subMap = when {
                lowerBound == null -> data.headMap(upperBound, upperInclusive)
                upperBound == null -> data.tailMap(lowerBound, lowerInclusive)
                else -> data.subMap(lowerBound, lowerInclusive, upperBound, upperInclusive)
            }
            val entries = subMap.values.flatten()

            try {
                return@buildLazySequence yieldAllFinal(entries.asSequence())
            }
            finally {
                dataLock.readLock.releaseFor(principal)
            }
        }
    }

    private val initializationRequested = AtomicBoolean(false)
    override fun shouldInitialize(): Boolean {
        val initialized = initializationRequested.compareAndExchange(false, true)
        return !initialized
    }

    class Loader : FactIndexImplementationLoader {
        override val implementationId = "storage.index.prologdb.in-memory"

        override fun createOrLoad(directoryManager: DataDirectoryManager.IndexScope): FactIndex {
            return MemoryIndex(directoryManager)
        }

        override fun destroy(directoryManager: DataDirectoryManager.IndexScope) {

        }

        override fun supportsFeature(feature: ImplFeature): Boolean {
            return ImplFeature.EFFICIENT_RANGE_QUERIES == feature
        }
    }
}