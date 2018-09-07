package com.github.prologdb.io.binaryprolog

import com.github.prologdb.runtime.term.*
import java.io.DataOutput
import kotlin.reflect.KClass

class BinaryPrologWriter {
    private val termWriters = mutableMapOf<KClass<out Term>, TermWriter<*>>()

    /**
     * Subsequence calls to XXX will use the given writer to write terms of
     * the given [termClass]. Overwrites previous invocations with the same
     * [termClass].
     */
    fun <T : Term> registerWriter(termClass: KClass<T>, writer: TermWriter<T>) {
        termWriters[termClass] = writer
    }

    /**
     * @throws BinaryPrologSerializationException
     */
    fun writeTermTo(term: Term, out: DataOutput) {
        val writer = findWriter(term::class)
            ?: throw BinaryPrologSerializationException("No writer for term class ${term::class.simpleName} registered.")

        writer.writeTermTo(term, out, this)
    }

    private fun <T : Term> findWriter(forType: KClass<out T>): TermWriter<T>? {
        val byDirectMatch = termWriters[forType]
        if (byDirectMatch != null) return byDirectMatch as TermWriter<T>

        val keyBySubtype = termWriters.keys.firstOrNull { it.java.isAssignableFrom(forType.java) }
        return if (keyBySubtype == null) null else termWriters[keyBySubtype] as TermWriter<T>
    }

    interface TermWriter<in T : Term> {
        val prologTypeName: String

        /**
         * Writes the given term to the given [DataOutput], including the type byte.
         * @param writerRef To be used for nested terms of unknown type
         */
        fun writeTermTo(term: T, out: DataOutput, writerRef: BinaryPrologWriter)
    }

    companion object {
        /**
         * @return a new instance of [BinaryPrologWriter] with the default configuration.
         */
        fun getDefaultInstance(): BinaryPrologWriter {
            val writer = BinaryPrologWriter()

            writer.registerWriter(PrologInteger::class, IntegerWriter)
            writer.registerWriter(PrologDecimal::class, DecimalWriter)
            writer.registerWriter(Variable::class, VariableWriter)
            writer.registerWriter(PrologString::class, StringWriter)
            writer.registerWriter(Atom::class, AtomWriter)
            writer.registerWriter(Predicate::class, PredicateWriter)
            writer.registerWriter(PrologList::class, ListWriter)
            writer.registerWriter(PrologDictionary::class, DictionaryWriter)

            return writer
        }
    }
}

fun DataOutput.writeIntEncoded(payload: Int) {
    if (payload <= 0b0111_1111) {
        writeByte(0b1000_0000 or payload)
    }
    else if (payload <= 0x3FFF) {
        writeByte((payload ushr 7) and 0b0111_1111)
        writeByte((payload and 0b0111_1111) or 0b1000_0000)
    }
    else if (payload <= 0x1F_FFFF) {
        writeByte((payload ushr 14) and 0b0111_1111)
        writeByte((payload ushr 7)  and 0b0111_1111)
        writeByte((payload and 0b0111_1111) or 0b1000_0000)
    }
    else if (payload <= 0xFFF_FFFF) {
        writeByte((payload ushr 21) and 0b0111_1111)
        writeByte((payload ushr 14) and 0b0111_1111)
        writeByte((payload ushr 7)  and 0b0111_1111)
        writeByte((payload and 0b0111_111) or 0b1000_0000)
    }
    else {
        writeByte((payload ushr 28) and 0b0000_1111)
        writeByte((payload ushr 21) and 0b0111_1111)
        writeByte((payload ushr 14) and 0b0111_1111)
        writeByte((payload ushr 7)  and 0b0111_1111)
        writeByte((payload and 0b0111_111) or 0b1000_0000)
    }
}

object IntegerWriter : BinaryPrologWriter.TermWriter<PrologInteger> {
    override val prologTypeName = "integer"

    private val TYPE_BYTE: Int = 0x10

    override fun writeTermTo(term: PrologInteger, out: DataOutput, writerRef: BinaryPrologWriter) {
        out.writeByte(TYPE_BYTE)
        out.writeIntEncoded(8)
        out.writeLong(term.value)
    }
}

object DecimalWriter : BinaryPrologWriter.TermWriter<PrologDecimal> {
    override val prologTypeName = "decimal"

    private val TYPE_BYTE: Int = 0x11

    override fun writeTermTo(term: PrologDecimal, out: DataOutput, writerRef: BinaryPrologWriter) {
        out.writeByte(TYPE_BYTE)
        out.writeIntEncoded(64)
        out.writeDouble(term.value)
    }
}

object VariableWriter : BinaryPrologWriter.TermWriter<Variable> {
    override val prologTypeName = "variable"

    private val TYPE_BYTE_REGULAR = 0x20
    private val TYPE_BYTE_ANONYMOUS = 0x20

    override fun writeTermTo(term: Variable, out: DataOutput, writerRef: BinaryPrologWriter) {
        if (term is AnonymousVariable || term.name == "_") {
            out.writeByte(TYPE_BYTE_ANONYMOUS)
        } else {
            out.writeByte(TYPE_BYTE_REGULAR)
            writeWithoutTypeByte(term, out)
        }
    }

    fun writeWithoutTypeByte(variable: Variable, out: DataOutput) {
        val encoded = Charsets.UTF_8.encode(variable.name)
        out.writeIntEncoded(encoded.limit())
        while (encoded.hasRemaining()) out.writeByte(encoded.get().toInt())
    }
}

object StringWriter : BinaryPrologWriter.TermWriter<PrologString> {
    override val prologTypeName = "string"

    private val TYPE_BYTE = 0x24

    override fun writeTermTo(term: PrologString, out: DataOutput, writerRef: BinaryPrologWriter) {
        val encoded = Charsets.UTF_8.encode(term.toKotlinString())
        out.writeByte(TYPE_BYTE)
        out.writeIntEncoded(encoded.limit())
        while (encoded.hasRemaining()) out.writeByte(encoded.get().toInt())
    }
}

object AtomWriter : BinaryPrologWriter.TermWriter<Atom> {
    override val prologTypeName = "atom"

    private val TYPE_BYTE = 0x22

    override fun writeTermTo(term: Atom, out: DataOutput, writerRef: BinaryPrologWriter) {
        out.writeByte(TYPE_BYTE)
        writeWithoutTypeByte(term.name, out)
    }

    fun writeWithoutTypeByte(atomName: String, out: DataOutput) {
        val encoded = Charsets.UTF_8.encode(atomName)
        out.writeIntEncoded(encoded.limit())
        while (encoded.hasRemaining()) out.writeByte(encoded.get().toInt())
    }
}

object PredicateWriter : BinaryPrologWriter.TermWriter<Predicate> {
    override val prologTypeName = "predicate"

    private val TYPE_BYTE = 0x30

    override fun writeTermTo(term: Predicate, out: DataOutput, writerRef: BinaryPrologWriter) {
        out.writeByte(TYPE_BYTE)
        out.writeIntEncoded(term.arity)
        AtomWriter.writeWithoutTypeByte(term.name, out)
        for (argument in term.arguments) {
            writerRef.writeTermTo(argument, out)
        }
    }
}

object ListWriter : BinaryPrologWriter.TermWriter<PrologList> {
    override val prologTypeName = "list"

    private val TYPE_BYTE_WITH_TAIL = 0x31
    private val TYPE_BYTE_WITHOUT_TAIL = 0x32

    override fun writeTermTo(term: PrologList, out: DataOutput, writerRef: BinaryPrologWriter) {
        val tail = term.tail
        if (tail == null) {
            writeWithoutTail(term.elements, out, writerRef)
        } else {
            writeWithTail(term.elements, tail, out, writerRef)
        }
    }

    private fun writeWithTail(elements: List<Term>, tail: Variable, out: DataOutput, writerRef: BinaryPrologWriter) {
        out.write(TYPE_BYTE_WITH_TAIL)
        VariableWriter.writeWithoutTypeByte(tail, out)
        out.writeIntEncoded(elements.size)
        for (element in elements) {
            writerRef.writeTermTo(element, out)
        }
    }

    private fun writeWithoutTail(elements: List<Term>, out: DataOutput, writerRef: BinaryPrologWriter) {
        out.write(TYPE_BYTE_WITHOUT_TAIL)
        out.writeIntEncoded(elements.size)
        for (element in elements) {
            writerRef.writeTermTo(element, out)
        }
    }
}

object DictionaryWriter : BinaryPrologWriter.TermWriter<PrologDictionary> {
    override val prologTypeName = "dictionary"

    private val TYPE_BYTE_WITH_TAIL = 0x40
    private val TYPE_BYTE_WITHOUT_TAIL = 0x41

    override fun writeTermTo(term: PrologDictionary, out: DataOutput, writerRef: BinaryPrologWriter) {
        val tail = term.tail

        if (tail == null) {
            writeWithoutTail(term.pairs, out, writerRef)
        } else {
            writeWithTail(term.pairs, tail, out, writerRef)
        }
    }

    private fun writeWithTail(entries: Map<Atom, Term>, tail: Variable, out: DataOutput, writerRef: BinaryPrologWriter) {
        out.writeByte(TYPE_BYTE_WITH_TAIL)
        VariableWriter.writeWithoutTypeByte(tail, out)
        out.writeIntEncoded(entries.size)
        for (entry in entries) {
            writeEntry(entry, out, writerRef)
        }
    }

    private fun writeWithoutTail(entries: Map<Atom, Term>, out: DataOutput, writerRef: BinaryPrologWriter) {
        out.writeByte(TYPE_BYTE_WITHOUT_TAIL)
        out.writeIntEncoded(entries.size)
        for (entry in entries) {
            writeEntry(entry, out, writerRef)
        }
    }

    private fun writeEntry(entry: Map.Entry<Atom, Term>, out: DataOutput, writerRef: BinaryPrologWriter) {
        AtomWriter.writeWithoutTypeByte(entry.key.name, out)
        writerRef.writeTermTo(entry.value, out)
    }
}