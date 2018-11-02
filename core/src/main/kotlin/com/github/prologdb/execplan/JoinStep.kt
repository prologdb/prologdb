package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.dbms.PrologDatabaseView
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class JoinStep(
        val subSteps: List<PlanStep>
) : PlanStep {
    override val execute: suspend LazySequenceBuilder<Unification>.(PrologDatabaseView, RandomVariableScope, VariableBucket) -> Unit = { db, randomVarsScope, variables ->
        runSteps(db, subSteps, randomVarsScope, variables)
    }

    override val explanation by lazy { Predicate("join", subSteps.map(PlanStep::explanation).toTypedArray()) }

    private suspend fun LazySequenceBuilder<Unification>.runSteps(db: PrologDatabaseView, steps: List<PlanStep>, randomVarsScope: RandomVariableScope, variables: VariableBucket) {
        if (steps.size == 1) {
            steps[0].execute(this, db, randomVarsScope, variables)
        }
        else {
            yieldAll(
                buildLazySequence<Unification>(principal) {
                    steps[0].execute(this, db, randomVarsScope, variables)
                }.flatMapRemaining { stepUnification ->
                    val variablesCarry = variables.copy()
                    variablesCarry.incorporate(stepUnification.variableValues)
                    runSteps(db, steps.subList(1, steps.lastIndex), randomVarsScope, variablesCarry)
                }
            )
        }
    }
}