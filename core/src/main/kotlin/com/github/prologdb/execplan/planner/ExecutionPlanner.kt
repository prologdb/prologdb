package com.github.prologdb.execplan.planner

import com.github.prologdb.dbms.PrologDatabaseView
import com.github.prologdb.execplan.PlanStep
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.query.Query

interface ExecutionPlanner {
    /**
     * Plans the execution of the given query against the given databaseView.
     */
    fun planExecution(query: Query, databaseView: PrologDatabaseView, randomVariableScope: RandomVariableScope = RandomVariableScope()): PlanStep
}