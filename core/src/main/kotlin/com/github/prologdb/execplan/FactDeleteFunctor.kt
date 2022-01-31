package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.dbms.SystemCatalog
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.storage.fact.FactStore
import com.github.prologdb.storage.fact.PersistenceID
import java.util.concurrent.Future

/**
 * Implements `+ -> fact_delete(indicator) -> void`
 */
class FactDeleteFunctor(
    val predicate: SystemCatalog.Predicate
) : PlanFunctor<PersistenceID, Unit> {
    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, PersistenceID>>): LazySequence<Pair<VariableBucket, Unit>> {
        val factStore = ctxt.getFactStore(predicate)
        
        return inputs.flatMapRemaining { (variableCarry, persistenceID) ->
            val present = await(delete(ctxt, predicate, persistenceID, factStore))
            if (present) Pair(variableCarry, Unit) else null
        }
    }

    override val explanation: CompoundTerm
        get() = CompoundTerm("fact_delete", arrayOf(predicate.indicator.toIdiomatic()))
}

/**
 * Implements `[+, fact] -> fact_delete_0(indicator) -> void`
 */
class FactDeleteFunctorOverload0(
    private val predicate: SystemCatalog.Predicate
) : PlanFunctor<Pair<PersistenceID, CompoundTerm>, Unit> {
    
    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Pair<PersistenceID, CompoundTerm>>>): LazySequence<Pair<VariableBucket, Unit>> {
        val factStore = ctxt.getFactStore(predicate)
        
        return inputs.flatMapRemaining { (variableCarry, pidAndFact) ->
            val present = await(delete(ctxt, predicate, pidAndFact.first, factStore))
            if (present) Pair(variableCarry, Unit) else null
        }
    }

    override val explanation: CompoundTerm
        get() = CompoundTerm("fact_delete_0", arrayOf(predicate.indicator.toIdiomatic()))
}

/**
 * Deletes the fact associated with `persistenceID` from the given `factStore` and all applying indices in `ctxt`.
 */
private fun delete(ctxt: DBProofSearchContext, predicate: SystemCatalog.Predicate, persistenceID: PersistenceID, factStore: FactStore): Future<Boolean> {
    return factStore.delete(ctxt.principal, persistenceID)
    // TODO: indices
}
