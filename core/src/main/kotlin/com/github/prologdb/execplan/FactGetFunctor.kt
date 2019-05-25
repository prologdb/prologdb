package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.storage.fact.PersistenceID

/**
 * Implements `+ -> fact_get(indicator) -> [+, fact]`
 */
class FactGetFunctor(
    val indicator: FullyQualifiedClauseIndicator
) : PlanFunctor<PersistenceID, Pair<PersistenceID, CompoundTerm>> {
    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, PersistenceID>>): LazySequence<Pair<VariableBucket, Pair<PersistenceID, CompoundTerm>>> {
        val factStore = ctxt.factStores[indicator.moduleName]?.get(indicator.indicator) ?: return LazySequence.fromGenerator {
            throw PrologQueryException("No fact store for $indicator")
        }
        
        return inputs.flatMapRemaining { (variableCarry, persistenceID) ->
            val fact = await(factStore.retrieve(principal, persistenceID))
            if (fact != null) {
                yield(Pair(
                    variableCarry,
                    Pair(
                        persistenceID,
                        fact
                    )
                ))
            }
        }
    }

    override val explanation: CompoundTerm
        get() = CompoundTerm("fact_get", arrayOf(indicator.toIdiomatic()))
}