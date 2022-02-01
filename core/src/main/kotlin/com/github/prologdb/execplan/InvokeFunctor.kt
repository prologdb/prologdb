package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.FullyQualifiedClauseIndicator
import com.github.prologdb.runtime.PrologStackTraceElement
import com.github.prologdb.runtime.prologTry
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class InvokeFunctor(
    val moduleName: String,
    val invocation: CompoundTerm,
    val stackTraceElementProvider: () -> PrologStackTraceElement
) : PlanFunctor<Any?, Unit> {
    val asColonTwo = CompoundTerm(":", arrayOf(
        Atom(moduleName),
        invocation
    ))
    val fullyQualifiedClauseIndicator = FullyQualifiedClauseIndicator(moduleName, ClauseIndicator.of(invocation))

    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Any?>>): LazySequence<Pair<VariableBucket, Unit>> {
        val (_, callable, _) = ctxt.resolveModuleScopedCallable(asColonTwo)
            ?: throw PrologRuntimeException("Predicate $fullyQualifiedClauseIndicator is not defined")

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