package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.dbms.SystemCatalog
import com.github.prologdb.runtime.FullyQualifiedClauseIndicator
import com.github.prologdb.runtime.PrologPermissionError
import com.github.prologdb.runtime.PrologStackTraceElement
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.storage.fact.PersistenceID

/**
 * Implements `* -> fact_scan(indicator) -> [+, fact]`.
 */
class FactScanFunctor(
    val predicate: SystemCatalog.Predicate,
    /** Is invoked on errors; the result will appear in the error stack trace. */
    stackFrameProvider: () -> PrologStackTraceElement
) : PlanFunctor<Any?, Pair<PersistenceID, CompoundTerm>>
{
    private val stackFrame by lazy(stackFrameProvider)
    
    override fun invoke(ctxt: DBProofSearchContext, inputs: LazySequence<Pair<VariableBucket, Any?>>): LazySequence<Pair<VariableBucket, Pair<PersistenceID, CompoundTerm>>> {
        if (!ctxt.authorization.mayRead(predicate.indicator)) {
            throw PrologPermissionError("Not allowed to read ${predicate.indicator}")
        }

        val factStore = ctxt.getFactStore(predicate)
        
        return inputs
            .flatMapRemaining<Pair<VariableBucket, Any?>, Pair<VariableBucket, Pair<PersistenceID, CompoundTerm>>> { (variableCarry, _) ->
                yieldAllFinal(
                    factStore.all(ctxt.principal)
                        .mapRemaining { (persistenceID, arguments) ->
                            Pair(variableCarry, Pair(persistenceID, CompoundTerm(predicate.functor, arguments)))
                        }
                )
            }
            .amendExceptionsWithStackTraceOnRemaining(stackFrame)
    }

    override val explanation: CompoundTerm
        get() = CompoundTerm("fact_scan", arrayOf(predicate.indicator.toIdiomatic()))
}
