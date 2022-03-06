package com.github.prologdb.execplan

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.async.launchWorkableFuture
import com.github.prologdb.dbms.PhysicalDatabaseProofSearchContext
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.VariableBucket

class DiscardFunctor<Input>(
    private val discarded: PlanFunctor<Input, *>
) : PlanFunctor<Input, Input> {

    override fun invoke(
        ctxt: PhysicalDatabaseProofSearchContext,
        inputs: LazySequence<Pair<VariableBucket, Input>>
    ): LazySequence<Pair<VariableBucket, Input>> {
        return inputs.flatMapRemaining { input ->
            await(launchWorkableFuture(ctxt.principal) {
                foldRemaining(discarded.invoke(ctxt, LazySequence.of(input)), Unit) { _, unit -> unit }
            })

            return@flatMapRemaining input
        }
    }

    override val explanation: CompoundTerm = CompoundTerm("|", arrayOf(
        discarded.explanation.apply {
            parenthesized = true
        },
        Atom("discard")
    ))
}