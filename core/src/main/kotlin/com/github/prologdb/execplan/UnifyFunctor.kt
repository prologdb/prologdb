package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.dbms.PhysicalDatabaseProofSearchContext
import com.github.prologdb.runtime.VariableMapping
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableDiscrepancyException
import com.github.prologdb.storage.fact.PersistenceID

/**
 * Implements `[+, fact] -> unify(fact) -> +` and `[+, fact] -> unify_filter(fact) -> +`
 */
class UnifyFunctor(
    val rhs: CompoundTerm,
    val instantiate: Boolean = true
) : PlanFunctor<Pair<PersistenceID, CompoundTerm>, PersistenceID> {

    private val rhsVariables = rhs.variables

    override fun invoke(ctxt: PhysicalDatabaseProofSearchContext, inputs: LazySequence<Pair<Unification, Pair<PersistenceID, CompoundTerm>>>): LazySequence<Pair<Unification, PersistenceID>> {
        val rhsMapping = VariableMapping()
        val randomRHS = ctxt.randomVariableScope.withRandomVariables(rhs, rhsMapping)
        return inputs
            .flatMapRemaining { (variableCarry, pidAndFact) ->
                val (persistenceID, fact) = pidAndFact
                val randomFact = ctxt.randomVariableScope.withRandomVariables(fact, VariableMapping())
                randomRHS.unify(randomFact, ctxt.randomVariableScope)?.let { unification ->
                    val resolvedBucket = unification.variableValues.withVariablesResolvedFrom(rhsMapping)
                    resolvedBucket.retainAll(rhsVariables)
                    try {
                        resolvedBucket.incorporate(variableCarry, ctxt.randomVariableScope)
                        if (instantiate) Pair(resolvedBucket, persistenceID) else Pair(variableCarry, persistenceID)
                    } catch (ex: VariableDiscrepancyException) {
                        // mismatch, do not yield (equals to prolog false)
                        null
                    }
                }
            }
    }
    
    override val explanation: CompoundTerm = CompoundTerm(if (instantiate) "unify" else "unify_filter", arrayOf(rhs))
}