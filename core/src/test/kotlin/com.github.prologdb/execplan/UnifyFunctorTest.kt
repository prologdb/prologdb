package com.github.prologdb.execplan

import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.async.remainingToList
import com.github.prologdb.dbms.PhysicalDatabaseProofSearchContext
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.PrologNumber
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.Unification
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import java.util.UUID

class UnifyFunctorTest : FreeSpec({
    val principal = UUID.randomUUID()
    
    "multiple inputs - all can be unified" {
        val inputs = buildLazySequence<Pair<Long, CompoundTerm>>(principal) {
            yield(Pair(0L, CompoundTerm("foo", arrayOf(PrologNumber(0)))))
            yield(Pair(1L, CompoundTerm("foo", arrayOf(PrologNumber(1)))))
            yield(Pair(2L, CompoundTerm("foo", arrayOf(PrologNumber(2)))))
            Pair(3L, CompoundTerm("foo", arrayOf(PrologNumber(3))))
        }
        val inputsWithVars = inputs.mapRemaining { Pair(Unification.TRUE, it) }
        val functor = UnifyFunctor(CompoundTerm("foo", arrayOf(Variable("A"))))
        val ctxt = mockk<PhysicalDatabaseProofSearchContext>()
        every { ctxt.randomVariableScope } returns RandomVariableScope()
        
        val results = functor.invoke(ctxt, inputsWithVars).remainingToList()
        
        results shouldBe listOf(
            Pair(varsBucket("A" to PrologNumber(0)), 0L),
            Pair(varsBucket("A" to PrologNumber(1)), 1L),
            Pair(varsBucket("A" to PrologNumber(2)), 2L),
            Pair(varsBucket("A" to PrologNumber(3)), 3L)
        )
    }

    "multiple inputs - some do not unify" {
        val inputs = buildLazySequence<Pair<Long, CompoundTerm>>(principal) {
            yield(Pair(0L, CompoundTerm("foo", arrayOf(PrologNumber(0)))))
            yield(Pair(1L, CompoundTerm("foo", arrayOf(PrologNumber(1), Atom("x")))))
            yield(Pair(2L, CompoundTerm("foo", arrayOf(PrologNumber(2)))))
            Pair(3L, CompoundTerm("foo", arrayOf(PrologNumber(3), Atom("y"))))
        }
        val inputsWithVars = inputs.mapRemaining { Pair(Unification.TRUE, it) }
        val functor = UnifyFunctor(CompoundTerm("foo", arrayOf(Variable("A"))))
        val ctxt = mockk<PhysicalDatabaseProofSearchContext>()
        every { ctxt.randomVariableScope } returns RandomVariableScope()

        val results = functor.invoke(ctxt, inputsWithVars).remainingToList()

        results shouldBe listOf(
            Pair(varsBucket("A" to PrologNumber(0)), 0L),
            Pair(varsBucket("A" to PrologNumber(2)), 2L)
        )
    }
})

private fun varsBucket(vararg of: Pair<String, Term>): Unification {
    val bucket = Unification.TRUE
    for ((varName, term) in of) {
        bucket.instantiate(Variable(varName), term)
    }
    
    return bucket
}