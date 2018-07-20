package com.github.prologdb.execplan

import com.github.prologdb.PredicateStore
import com.github.prologdb.PrologDatabase
import com.github.prologdb.indexing.IndexSet
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.lazysequence.*
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

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
    fun execute(db: PrologDatabase, randomVarsScope: RandomVariableScope, variables: VariableBucket): LazySequence<Unification>

    /**
     * An explanation of this step in the official format (e.g. `prove(bar(X, a))`
     */
    val explanation: Predicate
}

internal fun IndexSet.toLazySequence(store: PredicateStore): LazySequence<Predicate> {
    val indexIt = iterator()

    return LazySequence.fromGenerator {
        while (indexIt.hasNext()) {
            return@fromGenerator store.getByIndex(indexIt.next()) ?: continue
        }
        null
    }
}