package com.github.prologdb.execplan.planner

import com.github.prologdb.execplan.*
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.query.AndQuery
import com.github.prologdb.runtime.query.OrQuery
import com.github.prologdb.runtime.query.PredicateQuery
import com.github.prologdb.runtime.query.Query

/**
 * An execution planner that does no optimizations at all; behaves like a
 * stupid and simple prolog system; no indices are utilized.
 */
class NoOptimizationExecutionPlanner : ExecutionPlanner {
    override fun planExecution(query: Query, db: PlanningInformation, randomVariableScope: RandomVariableScope): PlanStep {
        return when(query) {
            is OrQuery -> UnionStep(query.goals.map { this.planExecution(it, db) })
            is AndQuery -> JoinStep(query.goals.map { this.planExecution(it, db) })
            is PredicateQuery -> {
                val indicator = ClauseIndicator.of(query.predicate)

                if (indicator in db.staticBuiltins) {
                    BuiltinInvocationStep(query.predicate)
                } else {
                    UnionStep(listOf(ScanStep(query.predicate), DeductionStep(query.predicate)))
                }
            }
            else -> throw PrologQueryException("Unsupported query type ${query::class.simpleName}")
        }
    }
}