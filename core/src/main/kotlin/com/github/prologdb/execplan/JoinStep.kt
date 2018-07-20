package com.github.prologdb.execplan

import com.github.prologdb.PrologDatabase
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.lazysequence.buildLazySequence
import com.github.prologdb.runtime.lazysequence.forEachRemaining
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class JoinStep(
        val subSteps: List<PlanStep>
) : PlanStep {
    override fun execute(db: PrologDatabase, randomVarsScope: RandomVariableScope, variables: VariableBucket): LazySequence<Unification> {
        return runSteps(db, subSteps, randomVarsScope, variables)
    }

    override val explanation by lazy { Predicate("join", subSteps.map(PlanStep::explanation).toTypedArray()) }

    private fun runSteps(db: PrologDatabase, steps: List<PlanStep>, randomVarsScope: RandomVariableScope, variables: VariableBucket): LazySequence<Unification> {
        if (steps.size == 1) return steps[0].execute(db, randomVarsScope, variables)
        var variablesCarry = variables.copy()

        return buildLazySequence {
            steps[0].execute(db, randomVarsScope, variables).forEachRemaining { stepUnification ->
                variablesCarry.incorporate(stepUnification.variableValues)

                yieldAll(runSteps(db, steps.subList(1, steps.lastIndex), randomVarsScope, variablesCarry))
            }
        }
    }
}