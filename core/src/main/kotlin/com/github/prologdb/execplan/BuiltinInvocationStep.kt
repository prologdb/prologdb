package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequenceBuilder
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

        if (builtinRule is NativeCodeRule) {
            builtinRule.callDirectly(this, invocation.arguments, ctxt)
        } else {
            builtinRule.unifyWithKnowledge(this, invocation, ctxt)
        }
    }

    override val explanation = Predicate("invoke_builtin", arrayOf(invocation))
}