package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.PrologPermissionError
import com.github.prologdb.runtime.VariableMapping
import com.github.prologdb.runtime.amendExceptionsWithStackTraceOnRemaining
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.toStackTraceElement
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class DeductionFunctor(
    val goal: CompoundTerm
) : PlanFunctor<Any?, Unit> {
    
    val goalIndicator = ClauseIndicator.of(goal)
    
    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Any?>>): LazySequence<Pair<VariableBucket, Unit>> {
        if (!ctxt.authorization.mayRead(goalIndicator)) {
            val ex = PrologPermissionError("Not allowed to read $goalIndicator")
            ex.addPrologStackFrame(goal.toStackTraceElement())
            throw ex
        }

        val rules = ctxt.rules[goalIndicator] ?: emptyList()
        
        if (rules.isEmpty()) return LazySequence.empty()
        
        return buildLazySequence(ctxt.principal) {
            for (rule in rules.toList()) { // to list to avoid concurrent modification exceptions
                yieldAll(
                    deduceWithRuleForFunctor(goal, rule, ctxt)
                )
            }
        }
    }

    override val explanation: CompoundTerm
        get() = CompoundTerm("deduce_from", arrayOf(goal))
}

/**
 * Calls the given rule with the given invocation `goal` within the given `context`.
 *
 * This is a toplevel fun so that it can be used within [DeductionStep] and [BuiltinInvocationStep]
 */
internal fun deduceWithRuleForFunctor(goal: CompoundTerm, rule: Rule, context: DBProofSearchContext): LazySequence<Pair<VariableBucket, Unit>> {
    val goalRandomMapping = VariableMapping()
    val randomGoal = context.randomVariableScope.withRandomVariables(goal, goalRandomMapping)

    val ruleRandomVarsMapping = VariableMapping()
    val randomHead = context.randomVariableScope.withRandomVariables(rule.head, ruleRandomVarsMapping)

    val goalAndHeadUnification = randomHead.unify(randomGoal)
    if (goalAndHeadUnification == null) {
        // this rule cannot be used to fulfill the given predicate
        return LazySequence.empty()
    }

    val randomQuery = rule.query
        .withRandomVariables(context.randomVariableScope, ruleRandomVarsMapping)
        .substituteVariables(goalAndHeadUnification.variableValues)

    return buildLazySequence<Unification>(context.principal) {
        context.fulfillAttach(this, randomQuery, VariableBucket())
    }
        .mapRemaining { unification ->
            val solutionVars = VariableBucket()

            for (randomGoalVariable in randomGoal.variables)
            {
                if (goalAndHeadUnification.variableValues.isInstantiated(randomGoalVariable)) {
                    val value = goalAndHeadUnification.variableValues[randomGoalVariable]
                        .substituteVariables(unification.variableValues.asSubstitutionMapper())
                        .substituteVariables(goalAndHeadUnification.variableValues.asSubstitutionMapper())

                    solutionVars.instantiate(randomGoalVariable, value)
                }
                else if (unification.variableValues.isInstantiated(randomGoalVariable)) {
                    val originalVar = goalRandomMapping.getOriginal(randomGoalVariable)!!
                    solutionVars.instantiate(originalVar, unification.variableValues[randomGoalVariable])
                }
            }

            Pair(
                solutionVars.withVariablesResolvedFrom(goalRandomMapping),
                Unit
            )
        }
        .amendExceptionsWithStackTraceOnRemaining(goal.toStackTraceElement())
}