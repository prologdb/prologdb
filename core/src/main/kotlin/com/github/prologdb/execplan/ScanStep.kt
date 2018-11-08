package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

/**
 * Most simple plan: scan all known predicates of a given [ClauseIndicator]
 */
class ScanStep(
    val goal: Predicate
) : PlanStep {

    private val goalIndicator = ClauseIndicator.of(goal)

    override val execute: suspend LazySequenceBuilder<Unification>.(PrologDatabaseView, RandomVariableScope, VariableBucket)-> Unit = { db, randomVarsScope, variables ->
        val predicateStore = db.predicateStores[goalIndicator]
        if (predicateStore != null) {
            yieldAll(predicateStore.all(principal)
                .mapRemaining { it.second.unify(goal, randomVarsScope) }
                .filterRemainingNotNull()
            )
        }
    }

    override val explanation by lazy { Predicate("prove", arrayOf(goal)) }
}