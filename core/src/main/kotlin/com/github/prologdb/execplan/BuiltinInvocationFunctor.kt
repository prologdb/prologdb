package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.builtin.NativeCodeRule
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.toStackTraceElement
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class BuiltinInvocationFunctor(
    val invocation: Predicate
) : PlanFunctor<Any?, Unit> {
    
    private val indicator = ClauseIndicator.of(invocation)
    
    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Any?>>): LazySequence<Pair<VariableBucket, Unit>> {
        val builtinRule = ctxt.staticBuiltins[indicator]
        if (builtinRule == null) {
            val ex = PrologRuntimeException("Cannot invoke builtin $indicator: not defined")
            ex.addPrologStackFrame(invocation.toStackTraceElement())
            throw ex
        }

        return inputs.flatMapRemaining { (vars, _) ->
            val replacedInvocation = invocation.substituteVariables(vars.asSubstitutionMapper())
            
            yieldAll(
                buildLazySequence<Unification>(ctxt.principal) {
                    if (builtinRule is NativeCodeRule) {
                        builtinRule.callDirectly(this, replacedInvocation.arguments, ctxt)
                    } else {
                        builtinRule.unifyWithKnowledge(this, replacedInvocation, ctxt)
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

    override val explanation: Predicate
        get() = Predicate("invoke_compiled", arrayOf(invocation))
}