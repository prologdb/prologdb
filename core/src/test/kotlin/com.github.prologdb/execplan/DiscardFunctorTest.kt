package com.github.prologdb.execplan

import com.github.prologdb.async.IrrelevantPrincipal
import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.flatMapRemaining
import com.github.prologdb.async.remainingToList
import com.github.prologdb.dbms.PhysicalDatabaseProofSearchContext
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.VariableBucket
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk

class DiscardFunctorTest : FreeSpec({
    "discards" {
        val psc = mockk<PhysicalDatabaseProofSearchContext> {
            every { principal } returns IrrelevantPrincipal
        }
        val bucketA = VariableBucket()
        bucketA.instantiate(Variable("N"), PrologNumber(1))
        val bucketB = VariableBucket()
        bucketB.instantiate(Variable("N"), PrologNumber(2))

        val source = listOf(Pair(bucketA, Unit), Pair(bucketB, Unit))
        var nSideEffects = 0

        val discardedStep = object : PlanFunctor<Unit, Unit> {
            override fun invoke(
                ctxt: PhysicalDatabaseProofSearchContext,
                inputs: LazySequence<Pair<VariableBucket, Unit>>
            ): LazySequence<Pair<VariableBucket, Unit>> {
                return inputs.flatMapRemaining { (inputBucket, _) ->
                    var modifiedBucket = VariableBucket()
                    modifiedBucket.incorporate(inputBucket, ctxt.randomVariableScope)
                    modifiedBucket.instantiate(Variable("D"), PrologNumber(1))

                    nSideEffects++
                    yield(Pair(modifiedBucket, Unit))

                    modifiedBucket = VariableBucket()
                    modifiedBucket.incorporate(inputBucket, ctxt.randomVariableScope)
                    modifiedBucket.instantiate(Variable("D"), PrologNumber(2))
                    nSideEffects++
                    Pair(modifiedBucket, Unit)
                }
            }

            override val explanation = CompoundTerm("to_be_discarded", arrayOf())
        }

        val fullPlan = DiscardFunctor(discardedStep)

        val results = fullPlan.invoke(psc, LazySequence.ofIterable(source)).remainingToList()
        results shouldBe source
        nSideEffects shouldBe 4
    }
})