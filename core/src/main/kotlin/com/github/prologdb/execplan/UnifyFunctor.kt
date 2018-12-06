package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.filterRemainingNotNull
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.VariableMapping
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.runtime.unification.VariableDiscrepancyException
import com.github.prologdb.storage.fact.PersistenceID

/**
 * Implements `[+, fact] -> unify(fact) -> +`
 */
class UnifyFunctor(
    val rhs: Predicate
) : PlanFunctor<Pair<PersistenceID, Predicate>, PersistenceID> {

    private val rhsVariables = rhs.variables

    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Pair<PersistenceID, Predicate>>>): LazySequence<Pair<VariableBucket, PersistenceID>> {
        val rhsMapping = VariableMapping()
        val randomRHS = ctxt.randomVariableScope.withRandomVariables(rhs, rhsMapping)
        return inputs
            .mapRemaining { (variableCarry, pidAndFact) ->
                val (persistenceID, fact) = pidAndFact
                val randomFact = ctxt.randomVariableScope.withRandomVariables(fact, VariableMapping())
                randomRHS.unify(randomFact)?.let { unification ->
                    val resolvedBucket = unification.variableValues.withVariablesResolvedFrom(rhsMapping)
                    resolvedBucket.retainAll(rhsVariables)
                    try {
                        resolvedBucket.incorporate(variableCarry)
                        Pair(resolvedBucket, persistenceID)
                    } catch (ex: VariableDiscrepancyException) {
                        null
                    }
                }
            }
            .filterRemainingNotNull()
    }
    
    override val explanation: Predicate
        get() = Predicate("unify", arrayOf(rhs))
}