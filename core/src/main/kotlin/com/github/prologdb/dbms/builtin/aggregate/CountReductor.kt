package com.github.prologdb.dbms.builtin.aggregate

import com.github.prologdb.async.WorkableFuture
import com.github.prologdb.parser.SyntaxError
import com.github.prologdb.parser.parser.ParseResult
import com.github.prologdb.parser.parser.ParseResultCertainty
import com.github.prologdb.parser.source.SourceLocation
import com.github.prologdb.runtime.proofsearch.ProofSearchContext
import com.github.prologdb.runtime.stdlib.aggregate.Reductor
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.PrologInteger
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.unification.VariableBucket
import java.util.concurrent.atomic.AtomicLong

class CountReductor : Reductor<Unit, AtomicLong, AtomicLong> {
    override fun parseSpecification(specification: Term): WorkableFuture<ParseResult<Unit>> {
        if (specification is Atom && specification.name == NAME) {
            return WorkableFuture.completed(ParseResult.of(Unit))
        }

        if (specification is CompoundTerm && specification.functor == NAME) {
            if (specification.arity == 0) {
                return WorkableFuture.completed(ParseResult.of(Unit))
            }

            return WorkableFuture.completed(ParseResult(Unit, ParseResultCertainty.MATCHED, setOf(SyntaxError(
                "The $NAME reductor does not take any arguments",
                specification.sourceInformation as? SourceLocation ?: SourceLocation.EOF,
            ))))
        }

        return WorkableFuture.completed(ParseResult(null, ParseResultCertainty.NOT_RECOGNIZED, emptySet()))
    }

    override fun initialize(ctxt: ProofSearchContext, specification: Unit) = WorkableFuture.completed(AtomicLong(0))
    override fun accumulate(ctxt: ProofSearchContext, accumulator: AtomicLong, element: VariableBucket): WorkableFuture<AtomicLong> {
        accumulator.incrementAndGet()
        return WorkableFuture.completed(accumulator)
    }
    override fun finalize(ctxt: ProofSearchContext, accumulator: AtomicLong) = WorkableFuture.completed(accumulator)
    override fun resultToTerm(ctxt: ProofSearchContext, result: AtomicLong) = PrologInteger(result.getAcquire())

    companion object {
        const val NAME = "count"
    }
}