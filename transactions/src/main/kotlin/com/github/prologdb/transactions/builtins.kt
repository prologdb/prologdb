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

val TransactionsLibrary : Library = object : SimpleLibrary(DoublyIndexedLibraryEntryStore(), DefaultOperatorRegistry()) {
    init {
        add(BeginTransactionBuiltin)
        add(CommitTransactionBuiltin)
        add(RollbackTransactionBuiltin)
    }
}