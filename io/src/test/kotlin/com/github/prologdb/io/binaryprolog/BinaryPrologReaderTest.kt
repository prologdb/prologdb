package com.github.prologdb.io.binaryprolog

import com.github.prologdb.runtime.query.AndQuery
import com.github.prologdb.runtime.query.OrQuery
import com.github.prologdb.runtime.query.PredicateInvocationQuery
import com.github.prologdb.runtime.term.AnonymousVariable
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.PrologBigNumber
import com.github.prologdb.runtime.term.PrologDictionary
import com.github.prologdb.runtime.term.PrologList
import com.github.prologdb.runtime.term.PrologLongInteger
import com.github.prologdb.runtime.term.PrologNumber
import com.github.prologdb.runtime.term.PrologString
import com.github.prologdb.runtime.term.Variable
import io.kotest.core.spec.style.FreeSpec
import io.kotest.inspectors.forOne
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import java.nio.ByteBuffer

class BinaryPrologReaderTest : FreeSpec({
    "integer" - {
        "variant a" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x10, 0x83.toByte(), 0x0E, 0xE3.toByte(), 0x4C))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 5
            result as PrologLongInteger
            result.value shouldBe 975692L
        }

        "variant b" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x10, 0x84.toByte(), 0x00, 0x0E, 0xE3.toByte(), 0x4C))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 6
            result as PrologLongInteger
            result.value shouldBe 975692L
        }

        "variant c" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x10, 0x85.toByte(), 0x00, 0x00, 0x0E, 0xE3.toByte(), 0x4C))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 7
            result as PrologLongInteger
            result.value shouldBe 975692L
        }

        "variant d" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x10, 0x88.toByte(), 0x00, 0x00, 0x00, 0x00, 0x00, 0x0E, 0xE3.toByte(), 0x4C))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 10
            result as PrologLongInteger
            result.value shouldBe 975692L
        }
    }

    "arbitrary-sized number" - {
        "314.15928" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x11, 0x01, 0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte(),
                0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte(), 0xFB.toByte(), 0x84.toByte(), 0x01,
                0xDF.toByte(), 0x5E, 0x78))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 15
            result as PrologBigNumber
            result shouldBe PrologBigNumber("314.15928")
        }

        "-1.6e-16" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x11, 0x00, 0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte(),
                0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte(), 0xEF.toByte(), 0x81.toByte(), 0x10))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 12
            result as PrologBigNumber
            result shouldBe PrologBigNumber("-1.6e-16")
        }

        "1e21" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x11, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x15,
                0x81.toByte(), 0x01))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 12
            result as PrologBigNumber
            result shouldBe PrologBigNumber("1e21")
        }

        "0" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x11, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x80.toByte()))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 11
            result as PrologBigNumber
            result shouldBe PrologBigNumber("0")
        }
    }

    "variable" - {
        "regular" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x20, 0x89.toByte(), 0x41, 0x76, 0x61, 0x72, 0x69, 0x61, 0x62, 0x6C, 0x65))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 11
            result as Variable
            result.name shouldBe "Avariable"
        }

        "anonymous" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x21))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 1
            result should beInstanceOf<AnonymousVariable>()
        }
    }

    "atom" {
        val buffer = ByteBuffer.wrap(byteArrayOf(0x22, 0x84.toByte(), 0x61, 0x74, 0x6F, 0x6D))

        val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

        buffer.position() shouldBe 6
        result as Atom
        result.name shouldBe "atom"
    }

    "string" - {
        "simple" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x24, 0x86.toByte(), 0x53, 0x74, 0x72, 0x69, 0x6E, 0x67))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 8
            result as PrologString
            result.toKotlinString() shouldBe "String"
        }

        "unicode specials" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x24, 0x87.toByte(), 0xE2.toByte(), 0x9E.toByte(),
                0xA9.toByte(), 0xF0.toByte(), 0x9F.toByte(), 0x99.toByte(), 0x8A.toByte()))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 9
            result as PrologString
            result.toKotlinString() shouldBe "➩\uD83D\uDE4A"
        }
    }

    "predicate" - {
        "variant a" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x30, 0x81.toByte(), 0x81.toByte(), 0x61, 0x22, 0x81.toByte(),
                0x78))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 7
            result as CompoundTerm
            result.arity shouldBe 1
            result.arguments[0] shouldBe Atom("x")
        }

        "variant b" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x30, 0x83.toByte(), 0x83.toByte(), 0x66,
                0x6F, 0x6F, 0x10, 0x81.toByte(), 0x01, 0x24, 0x83.toByte(), 0x62, 0x61,
                0x72, 0x22, 0x81.toByte(), 0x7A))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 17
            result as CompoundTerm
            result.arity shouldBe 3
            result.arguments[0] shouldBe PrologNumber(1)
            result.arguments[1] shouldBe PrologString("bar")
            result.arguments[2] shouldBe Atom("z")
        }
    }

    "list" - {
        "with tail" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x31, 0x81.toByte(), 0x54, 0x82.toByte(),
                0x22, 0x81.toByte(), 0x61, 0x10, 0x81.toByte(), 0x02))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 10
            result as PrologList
            result.elements.size shouldBe 2
            result.elements[0] shouldBe Atom("a")
            result.elements[1] shouldBe PrologNumber(2)
            result.tail shouldBe Variable("T")
        }

        "without tail" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x32, 0x82.toByte(), 0x22, 0x81.toByte(),
                0x61, 0x10, 0x81.toByte(), 0x02))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 8
            result as PrologList
            result.elements.size shouldBe 2
            result.elements[0] shouldBe Atom("a")
            result.elements[1] shouldBe PrologNumber(2)
            result.tail shouldBe null
        }
    }

    "dictionary" - {
        "with tail" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x40, 0x81.toByte(), 0x58, 0x81.toByte(), 0x81.toByte(), 0x61, 0x22, 0x81.toByte(), 0x62))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 9
            result as PrologDictionary
            result.pairs.size shouldBe 1

            assert(Atom("a") in result.pairs.keys)
            result.pairs[Atom("a")] shouldBe Atom("b")

            result.tail shouldBe Variable("X")
        }

        "without tail" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x41, 0x82.toByte(), 0x81.toByte(), 0x66,
                0x24, 0x81.toByte(), 0x62, 0x81.toByte(), 0x78, 0x10, 0x81.toByte(), 0x02))

            val result = BinaryPrologReader.getDefaultInstance().readTermFrom(buffer)

            buffer.position() shouldBe 12
            result as PrologDictionary
            result.pairs.size shouldBe 2

            assert(Atom("f") in result.pairs.keys)
            result.pairs[Atom("f")] shouldBe PrologString("b")

            assert(Atom("x") in result.pairs.keys)
            result.pairs[Atom("x")] shouldBe PrologNumber(2)

            result.tail shouldBe null
        }
    }

    "query" - {
        "A" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x60, 0x81.toByte(), 0x83.toByte(), 0x66,
                0x6F, 0x6F, 0x10, 0x81.toByte(), 0x05))

            val result = BinaryPrologReader.getDefaultInstance().readQueryFrom(buffer)

            buffer.position() shouldBe 9
            result as PredicateInvocationQuery

            result.goal shouldBe CompoundTerm("foo", arrayOf(PrologNumber(5)))
        }

        "B" {
            val buffer = ByteBuffer.wrap(byteArrayOf(0x61, 0x00, 0x82.toByte(), 0x61, 0x01,
                0x82.toByte(), 0x60, 0x81.toByte(), 0x83.toByte(), 0x66, 0x6F,
                0x6F, 0x20, 0x81.toByte(), 0x58, 0x60, 0x81.toByte(), 0x83.toByte(),
                0x62, 0x61, 0x72, 0x20, 0x81.toByte(), 0x58, 0x60, 0x81.toByte(),
                0x84.toByte(), 0x66, 0x75, 0x7A, 0x7A, 0x20, 0x81.toByte(), 0x59))

            val result = BinaryPrologReader.getDefaultInstance().readQueryFrom(buffer)

            buffer.position() shouldBe 34

            result as AndQuery
            result.goals.size shouldBe 2
            result.goals.forOne {
                it should beInstanceOf(PredicateInvocationQuery::class)
                it as PredicateInvocationQuery
                it.goal shouldBe CompoundTerm("fuzz", arrayOf(Variable("Y")))
            }
            result.goals.forOne { outer ->
                outer should beInstanceOf(OrQuery::class)
                outer as OrQuery
                outer.goals.size shouldBe 2
                outer.goals.forOne {
                    it should beInstanceOf(PredicateInvocationQuery::class)
                    it as PredicateInvocationQuery
                    it.goal shouldBe CompoundTerm("foo", arrayOf(Variable("X")))
                }
                outer.goals.forOne {
                    it should beInstanceOf(PredicateInvocationQuery::class)
                    it as PredicateInvocationQuery
                    it.goal shouldBe CompoundTerm("bar", arrayOf(Variable("X")))
                }
            }
        }
    }
})
