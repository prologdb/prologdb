package com.github.prologdb.transactions

import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.LibraryEntry
import com.github.prologdb.runtime.knowledge.library.LibraryEntryStore
import com.github.prologdb.runtime.knowledge.library.MutableLibraryEntryStore
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification

class LayeredInMemoryTransactionalLibraryEntryStore(val underlying: MutableLibraryEntryStore) : TransactionalLibraryEntryStore {

    /**
     * The current [TransactionLayer] (if a transaction is active), the naked [underlying] store otherwise.
     * When a new transaction is started, this is wrapped into a new [TransactionLayer]. When it stops,
     * the current layer is unwrapped.
     */
    private var currentLayer: MutableLibraryEntryStore = underlying
    private val currentLayerMutex = Any()

    override fun beginTransaction() {
        synchronized(currentLayerMutex) {
            currentLayer = TransactionLayer(currentLayer)
        }
    }

    override fun commit() {
        synchronized(currentLayerMutex) {
            val localCurrentLayer = currentLayer
            if (localCurrentLayer is TransactionLayer) {
                localCurrentLayer.apply()
                currentLayer = localCurrentLayer.underlying
            } else {
                throw TransactionException("Cannot commit: no transaction active.")
            }
        }
    }

    override fun rollback() {
        synchronized(currentLayerMutex) {
            val localCurrentLayer = currentLayer
            if (localCurrentLayer is TransactionLayer) {
                localCurrentLayer.discard()
                currentLayer = localCurrentLayer.underlying
            } else {
                throw TransactionException("Cannot roll back: no transaction active.")
            }
        }
    }

    override val exports: Iterable<LibraryEntry>
        get() = currentLayer.exports

    override fun abolish(functor: String, arity: Int) = currentLayer.abolish(functor, arity)

    override fun abolishFacts(functor: String, arity: Int) = currentLayer.abolishFacts(functor, arity)

    override fun add(entry: LibraryEntry) = currentLayer.add(entry)

    override fun findFor(predicate: Predicate): Iterable<LibraryEntry> = currentLayer.findFor(predicate)

    override fun include(other: LibraryEntryStore) = currentLayer.include(other)

    override fun retract(unifiesWith: Predicate): LazySequence<Unification> = currentLayer.retract(unifiesWith)

    override fun retractAll(unifiesWith: Predicate) = currentLayer.retractAll(unifiesWith)

    override fun retractAllFacts(fact: Predicate) = currentLayer.retractAllFacts(fact)

    override fun retractFact(fact: Predicate): LazySequence<Unification> = currentLayer.retractFact(fact)
}

private class TransactionLayer(val underlying: MutableLibraryEntryStore) : MutableLibraryEntryStore {

    private val additions: MutableList<LibraryEntry> = ArrayList()

    private val retractions: MutableList<LibraryEntry> = ArrayList()

    /** The actions in the order of execution. Used for correct commits (order matters!) */
    private val log: MutableList<TransactionLogEntry> = ArrayList()

    /** Is used to synchronize all operations */
    private val mutex = Any()

    override val exports: Iterable<LibraryEntry>
        get() {
            val nonRetracted = underlying.exports.asSequence()
                .filterNot(::isRetracted)
            return (nonRetracted + additions).asIterable()
        }

    override fun findFor(predicate: Predicate): Iterable<LibraryEntry> {
        val matchingAdditions = additions.asSequence()
            .filter { it.arity == predicate.arity && it.name == it.name }

        val underlyingNotRetraced = underlying.findFor(predicate).asSequence()
            .filterNot(::isRetracted)

        return (underlyingNotRetraced + matchingAdditions).asIterable()
    }

    override fun add(entry: LibraryEntry) {
        synchronized(mutex) {
            additions.add(entry)
            log.add(AdditionTransactionLogEntry(entry))
        }
    }

    override fun retract(unifiesWith: Predicate): LazySequence<Unification> {
        return LazySequence.fromGenerator {
            synchronized(mutex) {
                val entryToRetractWithUnification: Pair<LibraryEntry,Unification?>? = findFor(unifiesWith)
                    .asSequence()
                    .map { entry ->
                        if (entry is Predicate) {
                            Pair(entry, entry.unify(unifiesWith))
                        } else if (entry is Rule) {
                            Pair(entry, entry.head.unify(unifiesWith))
                        } else throw PrologRuntimeException("Found library entry not supported for retraction in transaction: $entry")
                    }
                    .firstOrNull { it.second != null }

                if (entryToRetractWithUnification != null) {
                    val (entry, unification) = entryToRetractWithUnification
                    retractions.add(entry)
                    log.add(SingleRetractionTransactionLogEntry(unifiesWith))
                    return@fromGenerator unification!!
                } else {
                    return@fromGenerator null
                }
            }
        }
    }

    override fun retractFact(fact: Predicate): LazySequence<Unification> {
        return LazySequence.fromGenerator {
            synchronized(mutex) {
                val entryToRetractWithUnification: Pair<LibraryEntry, Unification?>? = findFor(fact)
                    .asSequence()
                    .filter { it is Predicate }
                    .map { entry ->
                        if (entry is Predicate) {
                            Pair(entry, entry.unify(fact))
                        } else if (entry is Rule) {
                            Pair(entry, entry.head.unify(fact))
                        } else throw PrologRuntimeException("Found library entry not supported for retraction in transaction: $entry")
                    }
                    .firstOrNull { it.second != null }

                if (entryToRetractWithUnification != null) {
                    val (entry, unification) = entryToRetractWithUnification
                    retractions.add(entry)
                    log.add(SingleFactRetractionTransactionLogEntry(fact))
                    return@fromGenerator unification!!
                } else {
                    return@fromGenerator null
                }
            }
        }
    }

    /**
     * Applies all the cached changes to [underlying], effectively committing the transaction layer.
     */
    internal fun apply() {
        synchronized(mutex) {
            for (logEntry in log) {
                if (logEntry is AdditionTransactionLogEntry) {
                    underlying.add(logEntry.entry)
                } else if (logEntry is SingleRetractionTransactionLogEntry) {
                    val seq = underlying.retract(logEntry.unifiesWith)
                    seq.tryAdvance()
                    seq.close()
                } else if (logEntry is SingleFactRetractionTransactionLogEntry) {
                    val seq = underlying.retractFact(logEntry.fact)
                    seq.tryAdvance()
                    seq.close()
                } else {
                    // the log entry is not supported. it should never have been written to the log since the log is
                    // only accessible from within this class. 100% programmer error.
                    throw PrologRuntimeException("Internal error: transaction log contains unsupported operation")
                }
            }

            discard()
        }
    }

    /**
     * Discards all the cached changes to [underlying], theoretically rolling back the transaction layer. Also lets go
     * of any references so that they can be GCed sooner. Instances should not be used for other transactions
     * after this method has been invoked.
     *
     * Is also invoked by [apply] after the commit for GC reasons.
     */
    internal fun discard() {
        synchronized(mutex) {
            additions.clear()
            retractions.clear()
            log.clear()
        }
    }

    private fun isRetracted(possiblyRetraced: LibraryEntry) = retractions.any { retracted -> retracted === possiblyRetraced }
}