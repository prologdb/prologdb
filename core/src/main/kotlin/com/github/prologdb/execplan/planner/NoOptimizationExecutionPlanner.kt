package com.github.prologdb.execplan.planner

import com.github.prologdb.execplan.*
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.PrologStackTraceElement
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.query.AndQuery
import com.github.prologdb.runtime.query.OrQuery
import com.github.prologdb.runtime.query.PredicateInvocationQuery
import com.github.prologdb.runtime.query.Query

/**
 * An execution planner that does no optimizations at all; behaves like a
 * stupid and simple prolog system; no indices are utilized.
 */
class NoOptimizationExecutionPlanner : ExecutionPlanner {
    @Suppress("UNCHECKED_CAST")
    override fun planExecution(query: Query, db: PlanningInformation, randomVariableScope: RandomVariableScope): PlanFunctor<Unit, Any> {
        return when(query) {
            is OrQuery -> UnionFunctor(
                query.goals
                    .map { planExecution(it, db, randomVariableScope) }
                    .toTypedArray()
            )
            is AndQuery -> {
                when(query.goals.size) {
                    0 -> NoopFunctor() as PlanFunctor<Unit, Any>
                    1 -> planExecution(query.goals[0], db, randomVariableScope)
                    else -> {
                        var pivot = FunctorPipe(
                            planExecution(query.goals[0], db, randomVariableScope),
                            planExecution(query.goals[1], db, randomVariableScope) as PlanFunctor<Any, Any>
                        )

                        for (i in 2..query.goals.lastIndex) {
                            pivot = FunctorPipe(
                                pivot,
                                planExecution(query.goals[i], db, randomVariableScope) as PlanFunctor<Any, Any>
                            )
                        }

                        pivot
                    }
                }
            }
            is PredicateInvocationQuery -> {
                val indicator = ClauseIndicator.of(query.goal)
                val stackTraceElementProvider = {
                    PrologStackTraceElement(query.goal, query.sourceInformation)
                }

                if (indicator in db.staticBuiltins) {
                    TODO()
                } else {
                    FunctorPipe(
                        FactScanFunctor(indicator, stackTraceElementProvider),
                        UnifyFunctor(query.goal)
                    ) as PlanFunctor<Unit, Any>
                }
            }
            else -> throw PrologQueryException("Unsupported query type ${query::class.simpleName}")
        }
    }
}