package com.github.prologdb.execplan.planner

import com.github.prologdb.PrologDatabase
import com.github.prologdb.execplan.PlanStep
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.query.Query

interface ExecutionPlanner {
    /**
     * Plans the execution of the given query against the given database.
     */
    fun planExecution(query: Query, database: PrologDatabase, randomVariableScope: RandomVariableScope = RandomVariableScope()): PlanStep
}