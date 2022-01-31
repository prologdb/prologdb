package com.github.prologdb.execplan.planner

import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.dbms.SystemCatalog
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
    override fun planExecution(query: Query, ctxt: DBProofSearchContext, randomVariableScope: RandomVariableScope): PlanFunctor<Unit, Any> {
        return when(query) {
            is OrQuery -> UnionFunctor(
                query.goals
                    .map { planExecution(it, ctxt, randomVariableScope) }
                    .toTypedArray()
            )
            is AndQuery -> {
                when(query.goals.size) {
                    0 -> NoopFunctor() as PlanFunctor<Unit, Any>
                    1 -> planExecution(query.goals[0], ctxt, randomVariableScope)
                    else -> {
                        var pivot = FunctorPipe(
                            planExecution(query.goals[0], ctxt, randomVariableScope),
                            planExecution(query.goals[1], ctxt, randomVariableScope) as PlanFunctor<Any, Any>
                        )

                        for (i in 2..query.goals.lastIndex) {
                            pivot = FunctorPipe(
                                pivot,
                                planExecution(query.goals[i], ctxt, randomVariableScope) as PlanFunctor<Any, Any>
                            )
                        }

                        pivot
                    }
                }
            }
            is PredicateInvocationQuery -> {
                val (fqi, _, trueInvocation) = ctxt.resolveHead(query.goal)

                val predicateCatalog = ctxt.knowledgeBaseCatalog.allPredicatesByFqi[fqi]
                if (predicateCatalog != null) {
                    val stackTraceElementProvider = {
                        PrologStackTraceElement(query.goal, query.sourceInformation)
                    }

                    FunctorPipe(
                        FactScanFunctor(predicateCatalog, stackTraceElementProvider),
                        UnifyFunctor(trueInvocation)
                    ) as PlanFunctor<Unit, Any>
                } else {
                    InvokeFunctor(fqi.moduleName, trueInvocation) as PlanFunctor<Unit, Any>
                }
            }
            else -> throw PrologQueryException("Unsupported query type ${query::class.simpleName}")
        }
    }
}
