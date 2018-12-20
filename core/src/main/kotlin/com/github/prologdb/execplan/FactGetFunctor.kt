package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.storage.fact.PersistenceID

/**
 * Implements `+ -> fact_get(indicator) -> [+, fact]`
 */
class FactGetFunctor(
    val indicator: ClauseIndicator
) : PlanFunctor<PersistenceID, Pair<PersistenceID, Predicate>> {
    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, PersistenceID>>): LazySequence<Pair<VariableBucket, Pair<PersistenceID, Predicate>>> {
        val factStore = ctxt.factStores[indicator] ?: return LazySequence.fromGenerator { 
            throw PrologQueryException("No fact store for $indicator")
        }
        
        return inputs.flatMapRemaining { (variableCarry, persistenceID) ->
            val retrieved = await(factStore.retrieve(principal, persistenceID))
            
            if (retrieved != null) yield(Pair(
                variableCarry,
                Pair(
                    persistenceID,
                    retrieved
                )
            ))
        }
    }

    override val explanation: Predicate
        get() = Predicate("fact_get", arrayOf(indicator.toIdiomatic()))
}