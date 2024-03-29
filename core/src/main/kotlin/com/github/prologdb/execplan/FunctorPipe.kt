package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.dbms.PhysicalDatabaseProofSearchContext
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.VariableBucket

/**
 * The pipe between functors; this is the equivalent of a Prolog `,` or conjunction
 */
class FunctorPipe<Input, Intermediate, Output>(
    val first: PlanFunctor<Input, Intermediate>,
    val second: PlanFunctor<Intermediate, Output>
) : PlanFunctor<Input, Output> {
    override fun invoke(ctxt: PhysicalDatabaseProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Input>>): LazySequence<Pair<VariableBucket, Output>> {
        return second.invoke(ctxt, first.invoke(ctxt, inputs))
    }

    override val explanation: CompoundTerm
        get() = CompoundTerm("|", arrayOf(first.explanation, second.explanation))

    companion object {
        infix fun <Input, Intermediate, Output> PlanFunctor<Input, Intermediate>.into(into: PlanFunctor<Intermediate, Output>): PlanFunctor<Input, Output> {
            return FunctorPipe(this, into)
        }
    }
}
