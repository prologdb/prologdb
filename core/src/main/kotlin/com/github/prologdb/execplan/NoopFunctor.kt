package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.VariableBucket

/**
 * Implements `* -> noop -> *`
 */
class NoopFunctor : PlanFunctor<Any, Any> {
    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Any>>): LazySequence<Pair<VariableBucket, Any>> {
        return inputs
    }

    override val explanation: CompoundTerm
        get() = CompoundTerm("noop", emptyArray())
}