package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.VariableMapping
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class DeductionStep(
    val goal: Predicate
) : PlanStep {
    private val goalIndicator = ClauseIndicator.of(goal)

    override val execute: suspend LazySequenceBuilder<Unification>.(DBProofSearchContext, VariableBucket) -> Unit = { ctxt, vars ->
        val rules = ctxt.rules[goalIndicator] ?: emptyList()

        for (rule in rules.toList()) { // to list to avoid concurrent modification exceptions
            yieldAll(deduceWithRule(rule, ctxt))
        }
    }

    /** Internal override of [Rule.fulfill] */
    private fun deduceWithRule(rule: Rule, ctxt: DBProofSearchContext): LazySequence<Unification> {
        val predicateRandomVarsMapping = VariableMapping()
        val randomPredicate = ctxt.randomVariableScope.withRandomVariables(goal, predicateRandomVarsMapping)

        val ruleRandomVarsMapping = VariableMapping()
        val randomHead = ctxt.randomVariableScope.withRandomVariables(rule.head, ruleRandomVarsMapping)

        val predicateAndHeadUnification = randomHead.unify(randomPredicate)
        if (predicateAndHeadUnification == null) {
            // this rule cannot be used to fulfill the given predicate
            return Unification.NONE
        }

        val randomQuery = rule.query
            .withRandomVariables(ctxt.randomVariableScope, ruleRandomVarsMapping)
            .substituteVariables(predicateAndHeadUnification.variableValues)

        return buildLazySequence<Unification>(ctxt.principal) {
            ctxt.fulfillAttach(this, randomQuery, VariableBucket())
        }
        .mapRemaining { unification ->
            val solutionVars = VariableBucket()

            for (randomPredicateVariable in randomPredicate.variables)
            {
                if (predicateAndHeadUnification.variableValues.isInstantiated(randomPredicateVariable)) {
                    val value = predicateAndHeadUnification.variableValues[randomPredicateVariable]
                        .substituteVariables(unification.variableValues.asSubstitutionMapper())
                        .substituteVariables(predicateAndHeadUnification.variableValues.asSubstitutionMapper())

                    solutionVars.instantiate(randomPredicateVariable, value)
                }
                else if (unification.variableValues.isInstantiated(randomPredicateVariable)) {
                    val originalVar = predicateRandomVarsMapping.getOriginal(randomPredicateVariable)!!
                    solutionVars.instantiate(originalVar, unification.variableValues[randomPredicateVariable])
                }
            }

            Unification(solutionVars
                .withVariablesResolvedFrom(predicateRandomVarsMapping))
        }
    }

    override val explanation: Predicate by lazy { Predicate("deduce_from", arrayOf(goal)) }
}