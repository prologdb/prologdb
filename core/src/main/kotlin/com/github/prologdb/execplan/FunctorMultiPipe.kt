package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.async.forEachRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.VariableBucket

class FunctorMultiPipe<Input, Output>(
    val steps: Array<PlanFunctor<Input, Output>>
): PlanFunctor<Input, Output> {
    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Input>>): LazySequence<Pair<VariableBucket, Output>> {
        return buildLazySequence(ctxt.principal) {
            inputs.forEachRemaining { input ->
                for (step in steps) {
                    yieldAll(step.invoke(ctxt, LazySequence.singleton(input, principal)))
                }
            }
        }
    }

    override val explanation: Predicate
        get() = Predicate(";", steps.map { it.explanation }.toTypedArray())
}