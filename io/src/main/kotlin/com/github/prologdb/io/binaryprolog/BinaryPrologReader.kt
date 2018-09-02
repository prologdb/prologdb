package com.github.prologdb.io.binaryprolog

import com.github.prologdb.runtime.term.*
import java.nio.ByteBuffer

class BinaryPrologReader {
    private val termReaders = mutableMapOf<Byte, TermReader<*>>()

    /**
     * Subsequent calls to [readTermFrom] will use the given
     * term reader when the given typeByte is found. Overwrites
     * previous calls with the same typeByte.
     */
    fun setTermTypeReader(typeByte: Byte, reader: TermReader<*>) {
        termReaders[typeByte] = reader
    }

    /**
     * Reads the next term from the given byte buffer.
     *
     * When this method returns successfully, the byte buffer position must be on the
     * first byte following the read term.
     * When this method throws any sort of exception, the buffer must be
     * at the same position as it was when this method was invoked.
     */
    fun readTermFrom(buffer: ByteBuffer): Term {
        val initialPosition = buffer.position()
        val typeByte = buffer.get()
        val termReader = termReaders[typeByte]
        if (termReader == null) {
            buffer.position(initialPosition)
            throw BinaryPrologDeserializationException("Unknown term type byte value $typeByte; known: ${termReaders.keys.joinToString()}")
        }

        return try {
            termReader.readTermFrom(buffer, this)
        }
        catch (ex: Throwable) {
            buffer.position(initialPosition)
            throw BinaryPrologDeserializationException("Failed to read term of type ${termReader.prologTypeName} at position $initialPosition", ex)
        }
    }

    /**
     * Reads one type of term / responsible for exactly **one** type byte value.
     */
    interface TermReader<T : Term> {
        /**
         * The prolog type name of the terms this reader reads (e.g. integer, atom)
         */
        val prologTypeName: String

        /**
         * Reads the term from the given buffer. The type byte must haven been consumed before
         * invoking this method.
         * @param readerRef To be used when nested terms of unknown type are to be read.
         * @throws BinaryPrologDeserializationException
         */
        fun readTermFrom(buffer: ByteBuffer, readerRef: BinaryPrologReader): T
    }

    companion object {
        /**
         * @return A new [BinaryPrologReader] with the default configuration of [TermReader]s.
         */
        fun getDefaultInstance(): BinaryPrologReader {
            val reader = BinaryPrologReader()

            reader.setTermTypeReader(0x10, IntegerReader)
            reader.setTermTypeReader(0x11, DecimalReader)
            reader.setTermTypeReader(0x20, VariableReader)
            reader.setTermTypeReader(0x22, AtomReader)
            reader.setTermTypeReader(0x24, StringReader)
            reader.setTermTypeReader(0x30, PredicateReader)
            reader.setTermTypeReader(0x31, ListWithTailReader)
            reader.setTermTypeReader(0x32, ListWithoutTailReader)
            reader.setTermTypeReader(0x40, DictionaryWithTailReader)
            reader.setTermTypeReader(0x41, DictionaryWithoutTailReader)

            return reader
        }
    }
}

fun ByteBuffer.readEncodedIntegerAsInt(): Int {
    var value: Int = 0

    var nBytesFound = 0
    var hasNext: Boolean
    do {
        val rawByte = get()
        hasNext = rawByte > 0 // MSB is the sign bit on the JVM
        val significantBitsAsInt = rawByte.toInt() and 0b01111111
        value = value shl 7
        value += significantBitsAsInt
        nBytesFound++
    } while (hasNext && nBytesFound < 4)

    if (hasNext) {
        // too large
        throw BinaryPrologDeserializationException("Encoded integer is larger than expected maximum of 28 bit")
    }

    return value
}

object IntegerReader : BinaryPrologReader.TermReader<PrologInteger> {
    override val prologTypeName = "integer"

    override fun readTermFrom(buffer: ByteBuffer, readerRef: BinaryPrologReader): PrologInteger {
        val sizeInBytes = buffer.readEncodedIntegerAsInt()
        if (sizeInBytes > 8) {
            throw BinaryPrologDeserializationException("Integers of more than 8 bytes are not supported.")
        }

        var value: Long = 0
        for (lshiftFactor in sizeInBytes - 1 downTo 0) {
            val byteAsInt = buffer.get().toInt() and 0xFF
            value += byteAsInt shl (lshiftFactor * 8)
        }

        return PrologInteger.createUsingStringOptimizerCache(value)
    }
}

object DecimalReader : BinaryPrologReader.TermReader<PrologDecimal> {
    override val prologTypeName = "decimal"

    override fun readTermFrom(buffer: ByteBuffer, readerRef: BinaryPrologReader): PrologDecimal {
        val sizeInBits = buffer.readEncodedIntegerAsInt()

        val value: Double = when(sizeInBits) {
            32 -> buffer.float.toDouble()
            64 -> buffer.double
            else -> throw BinaryPrologDeserializationException("Decimals are only supported in IEEE-754 32bit and 64 bit format, found ${sizeInBits}bit decimal")
        }

        return PrologDecimal(value)
    }
}

object VariableReader : BinaryPrologReader.TermReader<Variable> {
    override val prologTypeName = "variable"

    override fun readTermFrom(buffer: ByteBuffer, readerRef: BinaryPrologReader): Variable {
        return Variable(readVariableNameFrom(buffer, readerRef))
    }

    private fun readVariableNameFrom(buffer: ByteBuffer, readerRef: BinaryPrologReader): String {
        val nUTF8Bytes = buffer.readEncodedIntegerAsInt()
        val dst = ByteArray(nUTF8Bytes)
        buffer.get(dst)
        return String(dst, Charsets.UTF_8)
    }
}

object StringReader : BinaryPrologReader.TermReader<PrologString> {
    override val prologTypeName = "string"

    override fun readTermFrom(buffer: ByteBuffer, readerRef: BinaryPrologReader): PrologString {
        val nUTF8Bytes = buffer.readEncodedIntegerAsInt()
        val dst = ByteArray(nUTF8Bytes)
        buffer.get(dst)
        val stringContent = String(dst, Charsets.UTF_8)
        return PrologString(stringContent)
    }
}

object AtomReader : BinaryPrologReader.TermReader<Atom> {
    override val prologTypeName = "atom"

    override fun readTermFrom(buffer: ByteBuffer, readerRef: BinaryPrologReader): Atom {
        return Atom(readAtomNameFrom(buffer, readerRef))
    }

    fun readAtomNameFrom(buffer: ByteBuffer, readerRef: BinaryPrologReader): String {
        val nUTF8Bytes = buffer.readEncodedIntegerAsInt()
        val dst = ByteArray(nUTF8Bytes)
        buffer.get(dst)
        return String(dst, Charsets.UTF_8)
    }
}

object PredicateReader : BinaryPrologReader.TermReader<Predicate> {
    override val prologTypeName = "predicate"

    override fun readTermFrom(buffer: ByteBuffer, readerRef: BinaryPrologReader): Predicate {
        val arity = buffer.readEncodedIntegerAsInt()
        val name = AtomReader.readAtomNameFrom(buffer, readerRef)
        val arguments = Array<Term?>(arity) { null }
        for (currentArgumentIndex in 0..arguments.lastIndex) {
            val term = readerRef.readTermFrom(buffer)
            arguments[currentArgumentIndex] = term
        }

        return Predicate(
            name,
            arguments as Array<Term>
        )
    }
}

object ListWithTailReader : BinaryPrologReader.TermReader<PrologList> {
    override val prologTypeName = "list"

    override fun readTermFrom(buffer: ByteBuffer, readerRef: BinaryPrologReader): PrologList {
        val tail = VariableReader.readTermFrom(buffer, readerRef)
        val size = buffer.readEncodedIntegerAsInt()
        val elementsList = ArrayList<Term>(size)
        for (elementIndex in 0 until size) {
            elementsList.add(readerRef.readTermFrom(buffer))
        }

        return PrologList(elementsList, tail)
    }
}

object ListWithoutTailReader : BinaryPrologReader.TermReader<PrologList> {
    override val prologTypeName = "list"

    override fun readTermFrom(buffer: ByteBuffer, readerRef: BinaryPrologReader): PrologList {
        val size = buffer.readEncodedIntegerAsInt()
        val elementsList = ArrayList<Term>(size)
        for (elementIndex in 0 until size) {
            elementsList.add(readerRef.readTermFrom(buffer))
        }

        return PrologList(elementsList, null)
    }
}

object DictionaryWithTailReader : BinaryPrologReader.TermReader<PrologDictionary> {
    override val prologTypeName = "dict"

    override fun readTermFrom(buffer: ByteBuffer, readerRef: BinaryPrologReader): PrologDictionary {
        val tail = VariableReader.readTermFrom(buffer, readerRef)
        val nKeys = buffer.readEncodedIntegerAsInt()
        val dictMap = HashMap<Atom, Term>(nKeys)
        for (iKey in 0 until nKeys) {
            val entry = readEntry(buffer, readerRef)
            dictMap[entry.first] = entry.second
        }

        return PrologDictionary(dictMap, tail)
    }

    internal fun readEntry(buffer: ByteBuffer, reader: BinaryPrologReader): Pair<Atom, Term> {
        val key = AtomReader.readTermFrom(buffer, reader)
        val value = reader.readTermFrom(buffer)
        return Pair(key, value)
    }
}

object DictionaryWithoutTailReader : BinaryPrologReader.TermReader<PrologDictionary> {
    override val prologTypeName = "dict"

    override fun readTermFrom(buffer: ByteBuffer, readerRef: BinaryPrologReader): PrologDictionary {
        val nKeys = buffer.readEncodedIntegerAsInt()
        val dictMap = HashMap<Atom, Term>(nKeys)
        for (iKey in 0 until nKeys) {
            val entry = DictionaryWithTailReader.readEntry(buffer, readerRef)
            dictMap[entry.first] = entry.second
        }

        return PrologDictionary(dictMap, null)
    }
}