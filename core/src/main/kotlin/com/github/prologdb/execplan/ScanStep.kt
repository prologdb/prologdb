package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.async.filterRemainingNotNull
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.*
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

    private val stackFrame by lazy {
        PrologStackTraceElement(
            goal,
            if (goal is HasPrologSource) goal.sourceInformation else NullSourceInformation
        )
    }

    override val execute: suspend LazySequenceBuilder<Unification>.(DBProofSearchContext, VariableBucket)-> Unit = { ctxt, vars ->
        if (!ctxt.authorization.mayRead(goalIndicator)) {
            throw PrologPermissionError("Not allowed to read $goalIndicator")
        }

        val factStore = ctxt.factStores[goalIndicator]
        if (factStore != null) {
            val amendedGoal = goal.substituteVariables(vars.asSubstitutionMapper())

            yieldAll(factStore.all(principal)
                .mapRemaining { it.second.unify(amendedGoal, ctxt.randomVariableScope) }
                .filterRemainingNotNull()
                .amendExceptionsWithStackTraceOnRemaining(stackFrame)
            )
        }
    }

    override val explanation by lazy { Predicate("prove", arrayOf(goal)) }
}