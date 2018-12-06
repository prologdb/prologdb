package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.PrologPermissionError
import com.github.prologdb.runtime.PrologStackTraceElement
import com.github.prologdb.runtime.amendExceptionsWithStackTraceOnRemaining
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.storage.fact.PersistenceID

/**
 * Implements `* -> fact_scan(indicator) -> [+, fact]`.
 */
class FactScanFunctor(
    val indicator: ClauseIndicator,
    /** Is invoked on errors; the result will appear in the error stack trace. */
    private val stackFrameProvider: () -> PrologStackTraceElement
) : PlanFunctor<Any?, Pair<PersistenceID, Predicate>>
{
    private val stackFrame by lazy(stackFrameProvider)
    
    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Any?>>): LazySequence<Pair<VariableBucket, Pair<PersistenceID, Predicate>>> {
        if (!ctxt.authorization.mayRead(indicator)) {
            throw PrologPermissionError("Not allowed to read $indicator")
        }

        val factStore = ctxt.factStores[indicator] ?: return LazySequence.empty()
        
        return inputs
            .flatMapRemaining<Pair<VariableBucket, Any?>, Pair<VariableBucket, Pair<PersistenceID, Predicate>>> { (variableCarry, _) ->
                factStore.all(ctxt.principal).flatMapRemaining<Pair<PersistenceID, Predicate>, Pair<VariableBucket, Pair<PersistenceID, Predicate>>> { (persistenceID, fact) ->
                    Pair(variableCarry, Pair(persistenceID, fact))
                }
            }
            .amendExceptionsWithStackTraceOnRemaining(stackFrame)
    }

    override val explanation: Predicate
        get() = Predicate("fact_scan", arrayOf(indicator.toIdiomatic()))
}