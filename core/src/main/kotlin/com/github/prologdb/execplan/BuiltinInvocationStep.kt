package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.builtin.NativeCodeRule
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.toStackTraceElement
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class BuiltinInvocationStep(
    val invocation: Predicate
) : PlanStep {

    val indicator = ClauseIndicator.of(invocation)

    override val execute: suspend LazySequenceBuilder<Unification>.(DBProofSearchContext, VariableBucket) -> Unit = { ctxt, vars ->
        val builtinRule = ctxt.staticBuiltins[indicator]
        if (builtinRule == null) {
            val ex = PrologRuntimeException("Cannot invoke builtin $indicator: not defined")
            ex.addPrologStackFrame(invocation.toStackTraceElement())
            throw ex
        }

        // incorporate existing instantiations
        val replacedInvocation = invocation.substituteVariables(vars.asSubstitutionMapper())

        yieldAll(buildLazySequence<Unification>(principal) {
            if (builtinRule is NativeCodeRule) {
                builtinRule.callDirectly(this, replacedInvocation.arguments, ctxt)
            } else {
                builtinRule.unifyWithKnowledge(this, replacedInvocation, ctxt)
            }
        }.mapRemaining { unification ->
            if (unification.variableValues.isEmpty) {
                Unification(vars)
            } else {
                val combinedVars = vars.copy()
                combinedVars.incorporate(unification.variableValues)
                Unification(combinedVars)
            }
        })
    }

    override val explanation = Predicate("invoke_builtin", arrayOf(invocation))
}