package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.PrologException
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.VariableBucket

interface PlanFunctor<Input, Output> {
    /**
     * Executes this functor for all given inputs.
     */
    @Throws(PrologQueryException::class, PrologException::class)
    fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Input>>): LazySequence<Pair<VariableBucket, Output>>
    
    /**
     *  An explanation of this step in the official format (e.g. `fact_scan(bar/1)`)
     */
    val explanation: Predicate
}