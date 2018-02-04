package com.github.prologdb.transactions

import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.knowledge.KnowledgeBase
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.DefaultOperatorRegistry
import com.github.prologdb.runtime.knowledge.library.DoublyIndexedLibraryEntryStore
import com.github.prologdb.runtime.knowledge.library.Library
import com.github.prologdb.runtime.knowledge.library.SimpleLibrary
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.query.PredicateQuery
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification

/**
 * If the [KnowledgeBase] this is called in implements [Transactional], invokes [Transactional.beginTransaction]
 * on it. Otherwise rises a [PrologRuntimeException] stating that the knowledge bases does not support
 * transactions.
 */
object BeginTransactionBuiltin : Rule(
    Predicate("transaction_begin", emptyArray()),
    PredicateQuery(Predicate("transaction_begin", emptyArray()))
) {
    override fun fulfill(predicate: Predicate, kb: KnowledgeBase, randomVariableScope: RandomVariableScope): LazySequence<Unification> {
        if (kb is Transactional<*>) {
            kb.beginTransaction()
            return Unification.SINGLETON
        } else {
            throw PrologRuntimeException("Transactions are not supported by this knowledge base.")
        }
    }
}

/**
 * If the [KnowledgeBase] this is called in implements [Transactional], invokes [Transactional.commit]
 * on it. Otherwise rises a [PrologRuntimeException] stating that the knowledge bases does not support
 * transactions.
 */
object CommitTransactionBuiltin : Rule(
    Predicate("transaction_commit", emptyArray()),
    PredicateQuery(Predicate("transaction_commit", emptyArray()))
) {
    override fun fulfill(predicate: Predicate, kb: KnowledgeBase, randomVariableScope: RandomVariableScope): LazySequence<Unification> {
        if (kb is Transactional<*>) {
            kb.commit()
            return Unification.SINGLETON
        } else {
            throw PrologRuntimeException("Transactions are not supported by this knowledge base.")
        }
    }
}

/**
 * If the [KnowledgeBase] this is called in implements [Transactional], invokes [Transactional.rollback]
 * on it. Otherwise rises a [PrologRuntimeException] stating that the knowledge bases does not support
 * transactions.
 */
object RollbackTransactionBuiltin : Rule(
    Predicate("transaction_rollback", emptyArray()),
    PredicateQuery(Predicate("transaction_rollback", emptyArray()))
) {
    override fun fulfill(predicate: Predicate, kb: KnowledgeBase, randomVariableScope: RandomVariableScope): LazySequence<Unification> {
        if (kb is Transactional<*>) {
            kb.rollback()
            return Unification.SINGLETON
        } else {
            throw PrologRuntimeException("Transactions are not supported by this knowledge base.")
        }
    }
}

/**
 * Bundles these bultins:
 *
 * * `transaction_begin/0` by importing [BeginTransactionBuiltin]
 * * `transaction_commit/0` by importing [CommitTransactionBuiltin]
 * * `transaction_rollback/0` by importing [RollbackTransactionBuiltin]
 *
 * In order for these builtins to actually work, the [KnowledgeBase] you intend to use them with **must** implement
 * [Transactional]. [LayeredInMemoryTransactionalLibraryEntryStore] will help with the implementation.
 */
val TransactionsLibrary : Library = object : SimpleLibrary(DoublyIndexedLibraryEntryStore(), DefaultOperatorRegistry()) {
    init {
        add(BeginTransactionBuiltin)
        add(CommitTransactionBuiltin)
        add(RollbackTransactionBuiltin)
    }
}