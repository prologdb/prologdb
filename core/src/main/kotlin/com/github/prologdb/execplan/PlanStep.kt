package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.async.Principal
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.dbms.PrologDatabaseView
import com.github.prologdb.indexing.PersistenceIDSet
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.storage.predicate.PredicateStore

/**
 * An element of an execution plan. Instances serve two purposes:
 * * provide the necessary inspection mechanisms/data so that the execution plan can be shown to the user
 * * execute their part of the execution plan
 */
interface PlanStep {
    /**
     * Loads the affected instances from the database and returns the unifications resulting from that.
     * @throws PrologQueryException
     */
    val execute: suspend LazySequenceBuilder<Unification>.(PrologDatabaseView, RandomVariableScope, VariableBucket) -> Unit

    /**
     * An explanation of this step in the official format (e.g. `prove(bar(X, a))`
     */
    val explanation: Predicate
}

internal fun PersistenceIDSet.toLazySequence(store: PredicateStore, principal: Principal): LazySequence<Predicate> {
    val indexIt = iterator()

    return buildLazySequence(principal) {
        while (indexIt.hasNext()) {
            yield(store.retrieve(indexIt.next()) ?: continue)
        }
    }
}