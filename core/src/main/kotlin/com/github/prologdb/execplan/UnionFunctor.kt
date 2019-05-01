package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.VariableBucket

class UnionFunctor<Input, Output>(
    private val steps: Array<PlanFunctor<Input, Output>>
) : PlanFunctor<Input, Output> {
    init {
        if (steps.isEmpty()) {
            throw IllegalArgumentException("The steps to UnionFunctor must not be empty")
        }
    }

    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Input>>): LazySequence<Pair<VariableBucket, Output>> {
        return buildLazySequence(ctxt.principal) {
            for (step in steps) {
                yieldAll(step.invoke(ctxt, inputs))
            }
        }
    }

    override val explanation: CompoundTerm
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
}
