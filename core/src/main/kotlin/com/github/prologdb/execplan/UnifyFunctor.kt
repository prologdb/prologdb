package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.VariableMapping
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.runtime.unification.VariableDiscrepancyException
import com.github.prologdb.storage.fact.PersistenceID

/**
 * Implements `[+, fact] -> unify(fact) -> +`
 */
class UnifyFunctor(
    val rhs: CompoundTerm
) : PlanFunctor<Pair<PersistenceID, CompoundTerm>, PersistenceID> {

    private val rhsVariables = rhs.variables

    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Pair<PersistenceID, CompoundTerm>>>): LazySequence<Pair<VariableBucket, PersistenceID>> {
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
                        resolvedBucket.incorporate(variableCarry)
                        yield(Pair(resolvedBucket, persistenceID))
                    } catch (ex: VariableDiscrepancyException) {
                        // mismatch, do not yield (equals to prolog false)
                    }
                }
            }
    }
    
    override val explanation: CompoundTerm
        get() = CompoundTerm("unify", arrayOf(rhs))
}