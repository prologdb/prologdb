package com.github.prologdb.execplan

import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.async.remainingToList
import com.github.prologdb.dbms.PhysicalDatabaseProofSearchContext
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.term.*
import com.github.prologdb.runtime.unification.VariableBucket
import io.kotlintest.matchers.shouldEqual
import io.kotlintest.specs.FreeSpec
import io.mockk.every
import io.mockk.mockk
import java.util.*

class UnifyFunctorTest : FreeSpec({
    val principal = UUID.randomUUID()
    
    "multiple inputs - all can be unified" {
        val inputs = buildLazySequence<Pair<Long, CompoundTerm>>(principal) {
            yield(Pair(0L, CompoundTerm("foo", arrayOf(PrologInteger(0)))))
            yield(Pair(1L, CompoundTerm("foo", arrayOf(PrologInteger(1)))))
            yield(Pair(2L, CompoundTerm("foo", arrayOf(PrologInteger(2)))))
            Pair(3L, CompoundTerm("foo", arrayOf(PrologInteger(3))))
        }
        val inputsWithVars = inputs.mapRemaining { Pair(VariableBucket(), it) }
        val functor = UnifyFunctor(CompoundTerm("foo", arrayOf(Variable("A"))))
        val ctxt = mockk<PhysicalDatabaseProofSearchContext>()
        every { ctxt.randomVariableScope } returns RandomVariableScope()
        
        val results = functor.invoke(ctxt, inputsWithVars).remainingToList()
        
        results shouldEqual listOf(
            Pair(varsBucket("A" to PrologInteger(0)), 0L),
            Pair(varsBucket("A" to PrologInteger(1)), 1L),
            Pair(varsBucket("A" to PrologInteger(2)), 2L),
            Pair(varsBucket("A" to PrologInteger(3)), 3L)
        )
    }

    "multiple inputs - some do not unify" {
        val inputs = buildLazySequence<Pair<Long, CompoundTerm>>(principal) {
            yield(Pair(0L, CompoundTerm("foo", arrayOf(PrologInteger(0)))))
            yield(Pair(1L, CompoundTerm("foo", arrayOf(PrologInteger(1), Atom("x")))))
            yield(Pair(2L, CompoundTerm("foo", arrayOf(PrologInteger(2)))))
            Pair(3L, CompoundTerm("foo", arrayOf(PrologInteger(3), Atom("y"))))
        }
        val inputsWithVars = inputs.mapRemaining { Pair(VariableBucket(), it) }
        val functor = UnifyFunctor(CompoundTerm("foo", arrayOf(Variable("A"))))
        val ctxt = mockk<PhysicalDatabaseProofSearchContext>()
        every { ctxt.randomVariableScope } returns RandomVariableScope()

        val results = functor.invoke(ctxt, inputsWithVars).remainingToList()

        results shouldEqual listOf(
            Pair(varsBucket("A" to PrologInteger(0)), 0L),
            Pair(varsBucket("A" to PrologInteger(2)), 2L)
        )
    }
})

private fun varsBucket(vararg of: Pair<String, Term>): VariableBucket {
    val bucket = VariableBucket()
    for ((varName, term) in of) {
        bucket.instantiate(Variable(varName), term)
    }
    
    return bucket
}