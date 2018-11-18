package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class JoinStep(
        val subSteps: List<PlanStep>
) : PlanStep {
    override val execute: suspend LazySequenceBuilder<Unification>.(DBProofSearchContext, VariableBucket) -> Unit = { ctxt, vars ->
        runSteps(ctxt, subSteps, vars)
    }

    override val explanation by lazy { Predicate("join", subSteps.map(PlanStep::explanation).toTypedArray()) }

    private suspend fun LazySequenceBuilder<Unification>.runSteps(ctxt: DBProofSearchContext, steps: List<PlanStep>, variables: VariableBucket) {
        if (steps.size == 1) {
            steps[0].execute(this, ctxt, variables)
        }
        else {
            yieldAll(
                buildLazySequence<Unification>(principal) {
                    steps[0].execute(this, ctxt, variables)
                }.flatMapRemaining { stepUnification ->
                    val variablesCarry = variables.copy()
                    variablesCarry.incorporate(stepUnification.variableValues)
                    runSteps(ctxt, steps.subList(1, steps.size), variablesCarry)
                }
            )
        }
    }
}