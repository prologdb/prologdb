package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.dbms.SystemCatalog
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.storage.fact.PersistenceID

/**
 * Implements `+ -> fact_get(indicator) -> [+, fact]`
 */
class FactGetFunctor(
    val predicate: SystemCatalog.Predicate
) : PlanFunctor<PersistenceID, Pair<PersistenceID, CompoundTerm>> {
    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, PersistenceID>>): LazySequence<Pair<VariableBucket, Pair<PersistenceID, CompoundTerm>>> {
        val factStore = ctxt.getFactStore(predicate)
        
        return inputs.flatMapRemaining { (variableCarry, persistenceID) ->
            await(factStore.retrieve(principal, persistenceID))
                ?.let { arguments ->
                    Pair(
                        variableCarry,
                        Pair(
                            persistenceID,
                            CompoundTerm(predicate.functor, arguments)
                        )
                    )
                }
            }
    }

    override val explanation: CompoundTerm
        get() = CompoundTerm("fact_get", arrayOf(predicate.indicator.toIdiomatic()))
}
