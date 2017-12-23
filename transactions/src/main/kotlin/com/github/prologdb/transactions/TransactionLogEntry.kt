package com.github.prologdb.transactions

import com.github.prologdb.runtime.knowledge.library.LibraryEntry
import com.github.prologdb.runtime.knowledge.library.MutableLibraryEntryStore
import com.github.prologdb.runtime.term.Predicate

/**
 * A mutating action that can be invoked on a [MutableLibraryEntryStore]. Is used to keep a log of actions taken
 * within a transaction.
 */
internal sealed class TransactionLogEntry {
    /**
     * Applies the change this action describes to the given store
     */
    abstract fun applyTo(store: MutableLibraryEntryStore)
}

/** @see [MutableLibraryEntryStore.add] */
internal data class AdditionTransactionLogEntry(val entry: LibraryEntry) : TransactionLogEntry() {
    override fun applyTo(store: MutableLibraryEntryStore) {
        store.add(entry)
    }
}

/** @see [MutableLibraryEntryStore.retractFact] */
internal data class SingleFactRetractionTransactionLogEntry(val fact: Predicate): TransactionLogEntry() {
    override fun applyTo(store: MutableLibraryEntryStore) {
        store.retractFact(fact)
    }
}

/** @see [MutableLibraryEntryStore.retract] */
internal data class SingleRetractionTransactionLogEntry(val unifiesWith: Predicate) : TransactionLogEntry() {
    override fun applyTo(store: MutableLibraryEntryStore) {
        store.retract(unifiesWith)
    }
}

/** @see [MutableLibraryEntryStore.retractAllFacts] */
internal data class AllFactsRetractionTransactionLogEntry(val fact: Predicate) : TransactionLogEntry() {
    override fun applyTo(store: MutableLibraryEntryStore) {
        store.retractAllFacts(fact)
    }
}

/** @see [MutableLibraryEntryStore.retractAll] */
internal data class AllRetractionTransactionLogEntry(val unifiesWith: Predicate) : TransactionLogEntry() {
    override fun applyTo(store: MutableLibraryEntryStore) {
        store.retractAll(unifiesWith)
    }
}

/** @see [MutableLibraryEntryStore.abolishFacts] */
internal data class FactAbolishmentTransactionLogEntry(val functor: String, val arity: Int): TransactionLogEntry() {
    init {
        if (arity < 0) throw IllegalArgumentException("Arity must be greater than or equal to 0")
    }

    override fun applyTo(store: MutableLibraryEntryStore) {
        store.abolishFacts(functor, arity)
    }
}

/** @see [MutableLibraryEntryStore.abolish] */
internal data class AbolishmentTransactionLogEntry(val functor: String, val arity: Int): TransactionLogEntry() {
    init {
        if (arity < 0) throw IllegalArgumentException("Arity must be greater than or equal to 0")
    }

    override fun applyTo(store: MutableLibraryEntryStore) {
        store.abolish(functor, arity)
    }
}