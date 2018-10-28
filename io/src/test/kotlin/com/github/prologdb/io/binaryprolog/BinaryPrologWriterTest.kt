package com.github.prologdb.io.binaryprolog

import com.github.prologdb.runtime.query.AndQuery
import com.github.prologdb.runtime.query.OrQuery
import com.github.prologdb.runtime.query.PredicateQuery
import com.github.prologdb.runtime.term.*
import io.kotlintest.shouldBe
import io.kotlintest.specs.FreeSpec
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

class BinaryPrologWriterTest : FreeSpec({
    "integer encoding" - {
        "one byte" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            out.writeIntEncoded(67)

            val bufferData = buffer.toByteArray()
            bufferData.size shouldBe 1
            bufferData[0].toInt() and 0xFF shouldBe 0xC3
        }

        "two bytes" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            out.writeIntEncoded(0x23AF)

            val bufferData = buffer.toByteArray()
            bufferData.size shouldBe 2
            bufferData[0].toInt() and 0xFF shouldBe 0x47
            bufferData[1].toInt() and 0xFF shouldBe 0xAF
        }

        "three bytes" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            out.writeIntEncoded(0x1A_007C)

            val bufferData = buffer.toByteArray()
            bufferData.size shouldBe 3
            bufferData[0].toInt() and 0xFF shouldBe 0x68
            bufferData[1].toInt() and 0xFF shouldBe 0x00
            bufferData[2].toInt() and 0xFF shouldBe 0xFC
        }

        "four bytes" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            out.writeIntEncoded(0xF20_BB39)
            // 1111001 0000010 1110110 0111001

            val bufferData = buffer.toByteArray()
            bufferData.size shouldBe 4
            bufferData[0].toInt() and 0xFF shouldBe 0x79
            bufferData[1].toInt() and 0xFF shouldBe 0x02
            bufferData[2].toInt() and 0xFF shouldBe 0x76
            bufferData[3].toInt() and 0xFF shouldBe 0xB9
        }

        "example A from spec" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            out.writeIntEncoded(59)

            val bufferData = buffer.toByteArray()
            bufferData.size shouldBe 1
            bufferData[0].toInt() and 0xFF shouldBe 0xBB
        }

        "example B from spec" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            out.writeIntEncoded(287)

            val bufferData = buffer.toByteArray()
            bufferData.size shouldBe 2
            bufferData[0].toInt() and 0xFF shouldBe 0x02
            bufferData[1].toInt() and 0xFF shouldBe 0x9F
        }
    }

    "integer" {
        val buffer = ByteArrayOutputStream()
        val out = DataOutputStream(buffer)

        BinaryPrologWriter.getDefaultInstance().writeTermTo(
            PrologInteger(4687792L),
            out
        )

        val bufferData = buffer.toByteArray()
        bufferData shouldBe byteArrayOf(0x10, 0x88.toByte(), 0x00, 0x00, 0x00, 0x00,
            0x00, 0x47, 0x87.toByte(), 0xB0.toByte())
    }

    "decimal" {
        val buffer = ByteArrayOutputStream()
        val out = DataOutputStream(buffer)

        BinaryPrologWriter.getDefaultInstance().writeTermTo(
            PrologDecimal(1.6e-16),
            out
        )

        val bufferData = buffer.toByteArray()
        bufferData shouldBe byteArrayOf(0x11, 0xC0.toByte(), 0x3C, 0xA7.toByte(), 0x0E,
            0xF5.toByte(), 0x46, 0x46, 0xD4.toByte(), 0x97.toByte())
    }

    "string" {
        val buffer = ByteArrayOutputStream()
        val out = DataOutputStream(buffer)

        BinaryPrologWriter.getDefaultInstance().writeTermTo(
            PrologString("String➩\uD83D\uDE4A"),
            out
        )

        val bufferData = buffer.toByteArray()
        bufferData shouldBe byteArrayOf(0x24, 0x8D.toByte(), 0x53, 0x74, 0x72, 0x69, 0x6E,
            0x67, 0xE2.toByte(), 0x9E.toByte(), 0xA9.toByte(), 0xF0.toByte(), 0x9F.toByte(),
            0x99.toByte(), 0x8A.toByte())
    }

    "atom" {
        val buffer = ByteArrayOutputStream()
        val out = DataOutputStream(buffer)

        BinaryPrologWriter.getDefaultInstance().writeTermTo(
            Atom("String➩\uD83D\uDE4A"),
            out
        )

        val bufferData = buffer.toByteArray()
        bufferData shouldBe byteArrayOf(0x22, 0x8D.toByte(), 0x53, 0x74, 0x72, 0x69, 0x6E,
            0x67, 0xE2.toByte(), 0x9E.toByte(), 0xA9.toByte(), 0xF0.toByte(), 0x9F.toByte(),
            0x99.toByte(), 0x8A.toByte())
    }

    "variable" {
        val buffer = ByteArrayOutputStream()
        val out = DataOutputStream(buffer)

        BinaryPrologWriter.getDefaultInstance().writeTermTo(
            Variable("String➩\uD83D\uDE4A"),
            out
        )

        val bufferData = buffer.toByteArray()
        bufferData shouldBe byteArrayOf(0x20, 0x8D.toByte(), 0x53, 0x74, 0x72, 0x69, 0x6E,
            0x67, 0xE2.toByte(), 0x9E.toByte(), 0xA9.toByte(), 0xF0.toByte(), 0x9F.toByte(),
            0x99.toByte(), 0x8A.toByte())
    }

    "predicate" - {
        "simple" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            BinaryPrologWriter.getDefaultInstance().writeTermTo(
                Predicate("a", arrayOf(Atom("x"))),
                out
            )

            val bufferData = buffer.toByteArray()
            bufferData shouldBe byteArrayOf(0x30, 0x81.toByte(), 0x81.toByte(), 0x61,
                0x22, 0x81.toByte(), 0x78.toByte())
        }

        "advanced" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            BinaryPrologWriter.getDefaultInstance().writeTermTo(
                Predicate("foo", arrayOf(
                    PrologInteger(1),
                    PrologString("bar"),
                    Atom("z")
                )),
                out
            )

            val bufferData = buffer.toByteArray()
            bufferData shouldBe byteArrayOf(0x30, 0x83.toByte(), 0x83.toByte(), 0x66,
                0x6F, 0x6F, 0x10, 0x88.toByte(), 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x01, 0x24, 0x83.toByte(), 0x62,
                0x61, 0x72, 0x22, 0x81.toByte(), 0x7A)
        }
    }

    "list" - {
        "with tail" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            BinaryPrologWriter.getDefaultInstance().writeTermTo(
                PrologList(
                    listOf(
                        Atom("a"),
                        PrologInteger(2)
                    ),
                    Variable("T")
                ),
                out
            )

            val bufferData = buffer.toByteArray()
            bufferData shouldBe byteArrayOf(0x31, 0x81.toByte(), 0x54, 0x82.toByte(),
                0x22, 0x81.toByte(), 0x61, 0x10, 0x88.toByte(), 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x02)
        }

        "without tail" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            BinaryPrologWriter.getDefaultInstance().writeTermTo(
                PrologList(
                    listOf(
                        Atom("a"),
                        PrologInteger(2)
                    )
                ),
                out
            )

            val bufferData = buffer.toByteArray()
            bufferData shouldBe byteArrayOf(0x32, 0x82.toByte(),
                0x22, 0x81.toByte(), 0x61, 0x10, 0x88.toByte(), 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x02)
        }
    }

    "dictionary" - {
        "with tail" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            BinaryPrologWriter.getDefaultInstance().writeTermTo(
                PrologDictionary(
                    mapOf(
                        Atom("a") to Atom("b")
                    ),
                    Variable("X")
                ),
                out
            )

            val bufferData = buffer.toByteArray()
            bufferData shouldBe byteArrayOf(0x40, 0x81.toByte(), 0x58, 0x81.toByte(),
                0x81.toByte(), 0x61, 0x22, 0x81.toByte(), 0x62)
        }

        "without tail" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            BinaryPrologWriter.getDefaultInstance().writeTermTo(
                PrologDictionary(
                    mapOf(
                        Atom("f") to PrologString("b"),
                        Atom("x") to PrologInteger(2)
                    )
                ),
                out
            )

            val bufferData = buffer.toByteArray()
            bufferData shouldBe byteArrayOf(0x41, 0x82.toByte(), 0x81.toByte(), 0x66,
                0x24, 0x81.toByte(), 0x62, 0x81.toByte(), 0x78, 0x10, 0x88.toByte(),
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)
        }
    }

    "query" - {
        "A" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            BinaryPrologWriter.getDefaultInstance().writeQueryTo(
                PredicateQuery(Predicate("foo", arrayOf(PrologInteger(5)))),
                out
            )

            val bufferData = buffer.toByteArray()
            bufferData shouldBe byteArrayOf(0x60, 0x81.toByte(), 0x83.toByte(), 0x66,
                0x6F, 0x6F, 0x10, 0x88.toByte(), 0, 0, 0, 0, 0, 0, 0, 0x05)
        }

        "B" {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)

            BinaryPrologWriter.getDefaultInstance().writeQueryTo(
                AndQuery(arrayOf(
                   OrQuery(arrayOf(
                       PredicateQuery(Predicate("foo", arrayOf(Variable("X")))),
                       PredicateQuery(Predicate("bar", arrayOf(Variable("X"))))
                   )),
                   PredicateQuery(Predicate("fuzz", arrayOf(Variable("Y"))))
                )),
                out
            )

            val bufferData = buffer.toByteArray()
            bufferData shouldBe byteArrayOf(0x61, 0x00, 0x82.toByte(), 0x61, 0x01,
                0x82.toByte(), 0x60, 0x81.toByte(), 0x83.toByte(), 0x66, 0x6F,
                0x6F, 0x20, 0x81.toByte(), 0x58, 0x60, 0x81.toByte(), 0x83.toByte(),
                0x62, 0x61, 0x72, 0x20, 0x81.toByte(), 0x58, 0x60, 0x81.toByte(),
                0x84.toByte(), 0x66, 0x75, 0x7A, 0x7A, 0x20, 0x81.toByte(), 0x59)
        }
    }
})