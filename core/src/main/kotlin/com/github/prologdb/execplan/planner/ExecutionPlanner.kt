package com.github.prologdb.execplan.planner

import com.github.prologdb.dbms.PhysicalDatabaseProofSearchContext
import com.github.prologdb.execplan.PlanFunctor
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.query.Query

interface ExecutionPlanner {
    /**
     * Plans the execution of the given query against the given database.
     */
    fun planExecution(query: Query, ctxt: PhysicalDatabaseProofSearchContext, randomVariableScope: RandomVariableScope): PlanFunctor<Unit, *>
}
