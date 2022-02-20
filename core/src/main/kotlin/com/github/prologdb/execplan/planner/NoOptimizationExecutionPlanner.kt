package com.github.prologdb.execplan.planner

import com.github.prologdb.dbms.PhysicalDatabaseProofSearchContext
import com.github.prologdb.dbms.builtin.PhysicalDynamicPredicate
import com.github.prologdb.execplan.FactScanFunctor
import com.github.prologdb.execplan.FunctorPipe
import com.github.prologdb.execplan.InvokeFunctor
import com.github.prologdb.execplan.NoopFunctor
import com.github.prologdb.execplan.PlanFunctor
import com.github.prologdb.execplan.PrologQueryException
import com.github.prologdb.execplan.UnifyFunctor
import com.github.prologdb.execplan.UnionFunctor
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.exception.PrologStackTraceElement
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
    override fun planExecution(query: Query, ctxt: PhysicalDatabaseProofSearchContext, randomVariableScope: RandomVariableScope): PlanFunctor<Unit, Any> {
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
                val (fqi, callable, trueInvocation) = ctxt.resolveHead(query.goal)
                val stackTraceElementProvider = {
                    PrologStackTraceElement(query.goal, query.sourceInformation)
                }

                if (callable is PhysicalDynamicPredicate) {
                    FunctorPipe(
                        FactScanFunctor(callable.catalog, stackTraceElementProvider),
                        UnifyFunctor(trueInvocation)
                    ) as PlanFunctor<Unit, Any>
                } else {
                    InvokeFunctor(fqi.moduleName, trueInvocation, callable, stackTraceElementProvider) as PlanFunctor<Unit, Any>
                }
            }
            else -> throw PrologQueryException("Unsupported query type ${query::class.simpleName}")
        }
    }
}
