package com.github.prologdb.execplan.planner

import com.github.prologdb.PrologDatabase
import com.github.prologdb.execplan.*
import com.github.prologdb.runtime.query.AndQuery
import com.github.prologdb.runtime.query.OrQuery
import com.github.prologdb.runtime.query.PredicateQuery
import com.github.prologdb.runtime.query.Query

/**
 * An execution planner that does no optimizations at all; behaves exactly like a
 * classic prolog system, no indices are utilized.
 */
class NoOptimizationExecutionPlanner : ExecutionPlanner {
    override fun planExecution(query: Query, database: PrologDatabase): PlanStep {
        return when(query) {
            is OrQuery -> UnionStep(query.goals.map { this.planExecution(it, database)})
            is AndQuery -> JoinStep(query.goals.map { this.planExecution(it, database) })
            is PredicateQuery -> UnionStep(listOf(ScanStep(query.predicate), DeductionStep(query.predicate)))
            else -> throw PrologQueryException("Unsupported query type ${query::class.simpleName}")
        }
    }
}