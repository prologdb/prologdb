package com.github.prologdb.execplan

import com.github.prologdb.PrologDatabaseView
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.lazysequence.buildLazySequence
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class UnionStep(
        val subSteps: List<PlanStep>
) : PlanStep {
    override fun execute(db: PrologDatabaseView, randomVarsScope: RandomVariableScope, variables: VariableBucket): LazySequence<Unification> {
        return buildLazySequence {
            for (step in subSteps) {
                yieldAll(step.execute(db, randomVarsScope, variables))
            }
        }
    }

    override val explanation by lazy { Predicate("union", subSteps.map(PlanStep::explanation).toTypedArray()) }
}