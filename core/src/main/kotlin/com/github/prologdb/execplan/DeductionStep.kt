package com.github.prologdb.execplan

import com.github.prologdb.PrologDatabaseView
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.VariableMapping
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.lazysequence.buildLazySequence
import com.github.prologdb.runtime.lazysequence.mapRemaining
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class DeductionStep(
    val goal: Predicate
) : PlanStep {
    private val goalIndicator = PredicateIndicator.of(goal)

    override fun execute(db: PrologDatabaseView, randomVarsScope: RandomVariableScope, variables: VariableBucket): LazySequence<Unification> {
        val rules = db.rules[goalIndicator] ?: return LazySequence.empty()

        return buildLazySequence {
            for (rule in rules.toList()) { // to list to avoid concurrent modification exceptions

            }
        }
    }

    /** Internal override of [Rule.fulfill] */
    private fun deduceWithRule(rule: Rule, db: PrologDatabaseView, randomVariableScope: RandomVariableScope): LazySequence<Unification> {
        val predicateRandomVarsMapping = VariableMapping()
        val randomPredicate = randomVariableScope.withRandomVariables(goal, predicateRandomVarsMapping)

        val ruleRandomVarsMapping = VariableMapping()
        val randomHead = randomVariableScope.withRandomVariables(rule.head, ruleRandomVarsMapping)

        val predicateAndHeadUnification = randomHead.unify(randomPredicate)
        if (predicateAndHeadUnification == null) {
            // this rule cannot be used to fulfill the given predicate
            return Unification.NONE
        }

        val randomQuery = rule.query
            .withRandomVariables(randomVariableScope, ruleRandomVarsMapping)
            .substituteVariables(predicateAndHeadUnification.variableValues)

        return db.execute(randomQuery, randomVariableScope)
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