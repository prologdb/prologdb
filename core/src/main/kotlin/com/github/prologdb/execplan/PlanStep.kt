package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

/**
 * An element of an execution plan. Instances serve two purposes:
 * * provide the necessary inspection mechanisms/data so that the execution plan can be shown to the user
 * * execute their part of the execution plan
 */
interface PlanStep {
    /**
     * Loads the affected instances from the database and returns the unifications resulting from that.
     * @throws PrologQueryException
     */
    val execute: suspend LazySequenceBuilder<Unification>.(DBProofSearchContext, VariableBucket) -> Unit

    /**
     * An explanation of this step in the official format (e.g. `prove(bar(X, a))`
     */
    val explanation: Predicate
}