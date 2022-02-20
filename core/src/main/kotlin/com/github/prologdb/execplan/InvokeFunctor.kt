package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.dbms.PhysicalDatabaseProofSearchContext
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.FullyQualifiedClauseIndicator
import com.github.prologdb.runtime.exception.PrologStackTraceElement
import com.github.prologdb.runtime.exception.prologTry
import com.github.prologdb.runtime.proofsearch.PrologCallable
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class InvokeFunctor(
    val moduleName: String,
    val invocation: CompoundTerm,
    val callable: PrologCallable,
    val stackTraceElementProvider: () -> PrologStackTraceElement
) : PlanFunctor<Any?, Unit> {
    val asColonTwo = CompoundTerm(":", arrayOf(
        Atom(moduleName),
        invocation
    ))
    val fqi = FullyQualifiedClauseIndicator(moduleName, ClauseIndicator.of(invocation))

    override fun invoke(ctxt: PhysicalDatabaseProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Any?>>): LazySequence<Pair<VariableBucket, Unit>> {
        return inputs.flatMapRemaining { (vars, _) ->
            val replacedInvocation = invocation.substituteVariables(vars.asSubstitutionMapper())
            yieldAllFinal(
                buildLazySequence<Unification>(ctxt.principal) {
                    prologTry(stackTraceElementProvider) {
                        callable.fulfill.invoke(this, replacedInvocation.arguments, ctxt)
                    }
                }
                    .mapRemaining { unification ->
                        if (unification.variableValues.isEmpty) {
                            Pair(vars, Unit)
                        } else {
                            val combinedVars = vars.copy()
                            combinedVars.incorporate(unification.variableValues)
                            Pair(combinedVars, Unit)
                        }
                    }
            )
        }
    }

    override val explanation: CompoundTerm
        get() = CompoundTerm("invoke", arrayOf(asColonTwo))
}