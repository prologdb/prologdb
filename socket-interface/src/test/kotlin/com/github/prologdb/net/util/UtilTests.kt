package com.github.prologdb.net.util

import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.VariableBucket
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.FreeSpec

class UtilTests : FreeSpec({
    "prolog inspection" - {
        "variable bucket topological sort" {
            // SETUP
            val bucket = VariableBucket()
            bucket.instantiate(Variable("C"), Atom("a"))
            bucket.instantiate(Variable("B"), Variable("C"))
            bucket.instantiate(Variable("A"), Variable("B"))

            // if this is the case then the variables are, by accident, sorted correctly
            // switching the names should be able to deterministically resolve this
            bucket.values.first() shouldNotBe Pair(Variable("A"), Variable("B"))

            // ACT
            val sorted = bucket.sortForSubstitution()

            var value = Predicate("foo", arrayOf(Variable("A")))
            sorted.forEach {  replacement ->
                value = value.substituteVariables(replacement.asSubstitutionMapper())
            }

            value shouldBe Predicate("foo", arrayOf(Atom("a")))
        }
    }
})