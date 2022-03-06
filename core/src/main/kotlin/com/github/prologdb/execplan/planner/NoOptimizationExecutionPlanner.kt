package com.github.prologdb.execplan.planner

import com.github.prologdb.dbms.PhysicalDatabaseProofSearchContext
import com.github.prologdb.dbms.builtin.BuiltinDatabaseRetract1
import com.github.prologdb.dbms.builtin.BuiltinDatabaseRetractAll1
import com.github.prologdb.dbms.builtin.PhysicalDynamicPredicate
import com.github.prologdb.execplan.DiscardFunctor
import com.github.prologdb.execplan.FactDeleteFunctor
import com.github.prologdb.execplan.FactScanFunctor
import com.github.prologdb.execplan.FunctorPipe
import com.github.prologdb.execplan.FunctorPipe.Companion.into
import com.github.prologdb.execplan.InvokeFunctor
import com.github.prologdb.execplan.NoopFunctor
import com.github.prologdb.execplan.PlanFunctor
import com.github.prologdb.execplan.PrologQueryException
import com.github.prologdb.execplan.UnifyFunctor
import com.github.prologdb.execplan.UnionFunctor
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.FullyQualifiedClauseIndicator
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.exception.PrologStackTraceElement
import com.github.prologdb.runtime.query.AndQuery
import com.github.prologdb.runtime.query.OrQuery
import com.github.prologdb.runtime.query.PredicateInvocationQuery
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.CompoundTerm

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

                when {
                    callable is PhysicalDynamicPredicate -> {
                        FunctorPipe(
                            FactScanFunctor(callable.catalog, stackTraceElementProvider),
                            UnifyFunctor(trueInvocation)
                        ) as PlanFunctor<Unit, Any>
                    }
                    fqi == FQI_RETRACT_1 || fqi == FQI_RETRACTALL_1 -> {
                        val retractionTargetTerm = trueInvocation.arguments[0]
                        if (retractionTargetTerm !is CompoundTerm) {
                            throw PrologQueryException("$fqi must be invoked on a compound term, but a term of type ${retractionTargetTerm.prologTypeName} is given")
                        }
                        val (retractFqi, retractCallable, retractionTargetTermResolved) = ctxt.resolveHead(retractionTargetTerm)
                        if (retractCallable !is PhysicalDynamicPredicate) {
                            throw PrologQueryException("Cannot retract clauses of $retractFqi: not a physical dynamic predicate")
                        }

                        val scan = FactScanFunctor(retractCallable.catalog, stackTraceElementProvider)
                        val delete = FactDeleteFunctor(retractCallable.catalog)

                        if (fqi == FQI_RETRACT_1) {
                            val unify = UnifyFunctor(retractionTargetTermResolved, true)

                            (scan into unify into delete) as PlanFunctor<Unit, Any>
                        } else {
                            val unify = UnifyFunctor(retractionTargetTermResolved, false)

                            DiscardFunctor(scan into unify into delete) as PlanFunctor<Unit, Any>
                        }
                    }
                    else -> InvokeFunctor(fqi.moduleName, trueInvocation, callable, stackTraceElementProvider) as PlanFunctor<Unit, Any>
                }
            }
            else -> throw PrologQueryException("Unsupported query type ${query::class.simpleName}")
        }
    }

    private companion object {
        val FQI_RETRACT_1 = FullyQualifiedClauseIndicator("\$clauses", ClauseIndicator.of(BuiltinDatabaseRetract1))
        val FQI_RETRACTALL_1 = FullyQualifiedClauseIndicator("\$clauses", ClauseIndicator.of(BuiltinDatabaseRetractAll1))
    }
}
