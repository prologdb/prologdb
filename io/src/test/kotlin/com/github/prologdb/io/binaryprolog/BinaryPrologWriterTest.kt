package com.github.prologdb.io.binaryprolog

import com.github.prologdb.runtime.term.PrologInteger
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
        TODO()
    }

    "string" {
        TODO()
    }

    "atom" {
        TODO()
    }

    "variable" {
        TODO()
    }

    "predicate" {
        TODO()
    }

    "list" - {
        "with tail" {
            TODO()
        }

        "without tail" {
            TODO()
        }
    }

    "dictionary" {
        "with tail" {
            TODO()
        }

        "without tail" {
            TODO()
        }
    }
})