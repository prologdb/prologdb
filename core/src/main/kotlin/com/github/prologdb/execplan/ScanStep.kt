package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.async.filterRemainingNotNull
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
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

    override val execute: suspend LazySequenceBuilder<Unification>.(DBProofSearchContext, VariableBucket)-> Unit = { ctxt, vars ->
        val amendedGoal = goal.substituteVariables(vars.asSubstitutionMapper())

        val predicateStore = ctxt.predicateStores[goalIndicator]
        if (predicateStore != null) {
            yieldAll(predicateStore.all(principal)
                .mapRemaining { it.second.unify(amendedGoal, ctxt.randomVariableScope) }
                .filterRemainingNotNull()
            )
        }
    }

    override val explanation by lazy { Predicate("prove", arrayOf(goal)) }
}