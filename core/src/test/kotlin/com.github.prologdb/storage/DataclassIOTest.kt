package com.github.prologdb.storage

import io.kotlintest.matchers.shouldEqual
import io.kotlintest.matchers.shouldThrowAny
import io.kotlintest.mock.mock
import io.kotlintest.specs.FreeSpec
import java.io.DataInput
import java.io.DataOutput
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer

class DataclassIOTest : FreeSpec() { init {
    "Buffer" - {
        "write struct" {
            val buffer = ByteBuffer.allocate(10)
            buffer.writeStruct(TestStruct(0x32211312, 0x355ACCBF))

            buffer.position(0)
            buffer.get() shouldEqual 0x32.toByte()
            buffer.get() shouldEqual 0x21.toByte()
            buffer.get() shouldEqual 0x13.toByte()
            buffer.get() shouldEqual 0x12.toByte()
            buffer.get() shouldEqual 0x35.toByte()
            buffer.get() shouldEqual 0x5A.toByte()
            buffer.get() shouldEqual 0xCC.toByte()
            buffer.get() shouldEqual 0xBF.toByte()
        }

        "read struct" {
            val buffer = ByteBuffer.allocate(10)
            buffer.position(0)
            buffer.put(0x32.toByte())
            buffer.put(0x21.toByte())
            buffer.put(0x13.toByte())
            buffer.put(0x12.toByte())
            buffer.put(0x35.toByte())
            buffer.put(0x5A.toByte())
            buffer.put(0xCC.toByte())
            buffer.put(0xBF.toByte())

            buffer.position(0)

            val struct = buffer.readStruct(TestStruct::class)

            struct shouldEqual TestStruct(0x32211312, 0x355ACCBF)
        }

        "write non-struct" {
            shouldThrowAny {
                ByteBuffer.allocate(10).writeStruct(NotAStruct(1, "foobar"))
            }
        }

        "read non-struct" {
            shouldThrowAny {
                ByteBuffer.allocate(10).readStruct(NotAStruct::class)
            }
        }
    }

    "DataInput & DataOutput" - {
        "write struct" {
            val file = File.createTempFile("yeehaw", "")
            val raf = RandomAccessFile(file, "rwd")
            raf.writeStruct(TestStruct(0x32211312, 0x355ACCBF))

            raf.seek(0)
            raf.readByte() shouldEqual 0x32.toByte()
            raf.readByte() shouldEqual 0x21.toByte()
            raf.readByte() shouldEqual 0x13.toByte()
            raf.readByte() shouldEqual 0x12.toByte()
            raf.readByte() shouldEqual 0x35.toByte()
            raf.readByte() shouldEqual 0x5A.toByte()
            raf.readByte() shouldEqual 0xCC.toByte()
            raf.readByte() shouldEqual 0xBF.toByte()
        }

        "read struct" {
            val file = File.createTempFile("yeehaw", "")
            val raf = RandomAccessFile(file, "rwd")
            raf.seek(0)
            raf.writeByte(0x32)
            raf.writeByte(0x21)
            raf.writeByte(0x13)
            raf.writeByte(0x12)
            raf.writeByte(0x35)
            raf.writeByte(0x5A)
            raf.writeByte(0xCC)
            raf.writeByte(0xBF)

            raf.seek(0)

            val struct = raf.readStruct(TestStruct::class)

            struct shouldEqual TestStruct(0x32211312, 0x355ACCBF)
        }

        "write non-struct" {
            shouldThrowAny {
                mock<DataOutput>().writeStruct(NotAStruct(1, "foobar"))
            }
        }

        "read non-struct" {
            shouldThrowAny {
                mock<DataInput>().readStruct(NotAStruct::class)
            }
        }
    }
}}

data class NotAStruct(
    val a: Int,
    val b: String
)

data class TestStruct(
    val a: Int,
    val b: Int
)