package com.github.prologdb.net.util

import com.github.prologdb.runtime.CircularTermException
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.Unification
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe

class UtilTests : FreeSpec({
    "prolog inspection" - {
        "variable bucket topological sort" - {
            "happy path" {
                // SETUP
                val bucket = Unification.TRUE
                bucket.instantiate(Variable("C"), Atom("a"))
                bucket.instantiate(Variable("B"), Variable("C"))
                bucket.instantiate(Variable("A"), Variable("B"))

                // if this is the case then the variables are, by accident, sorted correctly
                // switching the names should be able to deterministically resolve this
                bucket.values.first() shouldNotBe Pair(Variable("A"), Variable("B"))

                // ACT
                val sorted = bucket.sortedForSubstitution()

                var value = CompoundTerm("foo", arrayOf(Variable("A")))
                sorted.forEach { replacement ->
                    value = value.substituteVariables(replacement.asSubstitutionMapper())
                }

                value shouldBe CompoundTerm("foo", arrayOf(Atom("a")))
            }

            "circular dependency should error" {
                // SETUP
                val bucket = Unification.TRUE
                bucket.instantiate(Variable("A"), CompoundTerm("foo", arrayOf(Variable("B"))))
                bucket.instantiate(Variable("B"), CompoundTerm("bar", arrayOf(Variable("A"))))

                // ACT & ASSERT
                shouldThrow<CircularTermException> {
                    bucket.sortedForSubstitution()
                }
            }
        }
    }
})