package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.dbms.PhysicalDatabaseProofSearchContext
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.VariableBucket

/**
 * Concatenates the output of multiple steps. This is the quivalent of a prolog `;` or SQL UNION
 */
class UnionFunctor<Input, Output>(
    private val steps: Array<PlanFunctor<Input, Output>>
) : PlanFunctor<Input, Output> {
    init {
        if (steps.isEmpty()) {
            throw IllegalArgumentException("The steps to UnionFunctor must not be empty")
        }
    }

    override fun invoke(ctxt: PhysicalDatabaseProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Input>>): LazySequence<Pair<VariableBucket, Output>> {
        if (steps.size == 1) {
            return steps.single().invoke(ctxt, inputs)
        }

        return buildLazySequence(ctxt.principal) {
            for (stepIndex in 0 until steps.lastIndex) {
                yieldAll(steps[stepIndex].invoke(ctxt, inputs))
            }

            return@buildLazySequence yieldAllFinal(steps.last().invoke(ctxt, inputs))
        }
    }

    override val explanation: CompoundTerm
        get() = CompoundTerm(";", steps.map { it.explanation }.toTypedArray())
}
