package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.VariableBucket

interface PlanFunctor<Input, Output> {

    /**
     * Executes this functor for the given input.
     */
    fun invoke(ctxt: DBProofSearchContext, variableCarry: VariableBucket, input: Input): LazySequence<Pair<VariableBucket, Output>>
    
    /**
     *  An explanation of this step in the official format (e.g. `fact_scan(bar/1)`)
     */
    val explanation: Predicate
}