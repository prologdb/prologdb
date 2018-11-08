package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class UnionStep(
        val subSteps: List<PlanStep>
) : PlanStep {

    override val execute: suspend LazySequenceBuilder<Unification>.(DBProofSearchContext, VariableBucket) -> Unit = { ctxt, vars ->
        for (step in subSteps) {
            step.execute(this, ctxt, vars)
        }
    }

    override val explanation by lazy { Predicate("union", subSteps.map(PlanStep::explanation).toTypedArray()) }
}