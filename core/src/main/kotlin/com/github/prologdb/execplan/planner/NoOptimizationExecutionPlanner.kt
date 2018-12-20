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
    override fun planExecution(query: Query, db: PlanningInformation, randomVariableScope: RandomVariableScope): PlanFunctor<Unit, *> {
        return planExecutionInternal(query, db, randomVariableScope) as PlanFunctor<Unit, *>
    }
    
    private fun planExecutionInternal(query: Query, db: PlanningInformation, randomVariableScope: RandomVariableScope): PlanFunctor<Any?, Any?> {
        return when(query) {
            is OrQuery -> planUnionExecution(query, db, randomVariableScope)
            is AndQuery -> planJoinExecution(query, db, randomVariableScope)
            is PredicateQuery -> planPredicateLookup(query, db, randomVariableScope)
            else -> throw PrologQueryException("Unsupported query type ${query::class.simpleName}")
        }
    }
    
    private fun planUnionExecution(query: OrQuery, db: PlanningInformation, randomVariableScope: RandomVariableScope): PlanFunctor<Any?, Any?> {
        return UnionFunctor(query.goals.map { planExecutionInternal(it, db, randomVariableScope) }.toTypedArray())
    }
    
    private fun planJoinExecution(query: AndQuery, db: PlanningInformation, randomVariableScope: RandomVariableScope): PlanFunctor<Any?, Any?> {
        val goals = query.goals
        return when (goals.size) {
            in Int.MIN_VALUE..0 -> throw RuntimeException("Cannot plan an ${AndQuery::class.qualifiedName} with no goals.")
                              1 -> planExecutionInternal(goals[0], db, randomVariableScope)
                           else -> {
                               val first = JoinFunctor(
                                   planExecutionInternal(goals[0], db, randomVariableScope),
                                   planExecutionInternal(goals[1], db, randomVariableScope)
                               )
                               
                               return goals.asSequence()
                                   .drop(2)
                                   .fold(first) { fnc, q -> JoinFunctor(fnc, planExecutionInternal(q, db, randomVariableScope))}
                           }
        }
    }
    
    private fun planPredicateLookup(query: PredicateQuery, db: PlanningInformation, randomVariableScope: RandomVariableScope): PlanFunctor<Any?, Any?> {
        val indicator = ClauseIndicator.of(query.predicate)

        return if (indicator in db.staticBuiltins) {
            BuiltinInvocationFunctor(query.predicate) as PlanFunctor<Any?, Any?>
        } else {
            UnionFunctor(arrayOf(
                JoinFunctor(
                    FactScanFunctor(ClauseIndicator.of(query.predicate), query::stackFrame),
                    UnifyFunctor(query.predicate)
                ) as PlanFunctor<Any?, Any?>,
                DeductionFunctor(query.predicate) as PlanFunctor<Any?, Any?>
            ))
        }
    }
}