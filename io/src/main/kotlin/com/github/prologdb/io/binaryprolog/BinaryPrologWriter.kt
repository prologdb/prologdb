package com.github.prologdb.io.binaryprolog

import com.github.prologdb.runtime.query.AndQuery
import com.github.prologdb.runtime.query.OrQuery
import com.github.prologdb.runtime.query.PredicateInvocationQuery
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.AnonymousVariable
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.PrologBigNumber
import com.github.prologdb.runtime.term.PrologDictionary
import com.github.prologdb.runtime.term.PrologList
import com.github.prologdb.runtime.term.PrologLongInteger
import com.github.prologdb.runtime.term.PrologString
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.term.Variable
import java.io.DataOutput

class BinaryPrologWriter {
    private val termWriters = mutableMapOf<Class<out Term>, TermWriter<*>>()

    /**
     * Subsequence calls to XXX will use the given writer to write terms of
     * the given [termClass]. Overwrites previous invocations with the same
     * [termClass].
     */
    fun <T : Term> registerWriter(termClass: Class<T>, writer: TermWriter<T>) {
        termWriters[termClass] = writer
    }

    /**
     * @throws BinaryPrologSerializationException
     */
    fun writeTermTo(term: Term, out: DataOutput) {
        val writer = findWriter(term::class.java)
            ?: throw BinaryPrologSerializationException("No writer for term class ${term::class.java.simpleName} registered.")

        writer.writeTermTo(term, out, this)
    }

    fun writeQueryTo(query: Query, out: DataOutput) {
        QueryWriter.writeQueryTo(query, out, this)
    }

    private fun <T : Term> findWriter(forType: Class<out T>): TermWriter<T>? {
        val byDirectMatch = termWriters[forType]
        @Suppress("UNCHECKED_CAST")
        if (byDirectMatch != null) return byDirectMatch as TermWriter<T>

        return termWriters.keys
            .firstOrNull { it.isAssignableFrom(forType) }
            ?.let { keyBySubtype ->
                @Suppress("UNCHECKED_CAST")
                termWriters[keyBySubtype] as TermWriter<T>
            }
    }

    interface TermWriter<in T : Term> {
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

            writer.registerWriter(PrologLongInteger::class.java, LongIntegerWriter)
            writer.registerWriter(PrologBigNumber::class.java, BigNumberWriter)
            writer.registerWriter(Variable::class.java, VariableWriter)
            writer.registerWriter(PrologString::class.java, StringWriter)
            writer.registerWriter(Atom::class.java, AtomWriter)
            writer.registerWriter(CompoundTerm::class.java, PredicateWriter)
            writer.registerWriter(PrologList::class.java, ListWriter)
            writer.registerWriter(PrologDictionary::class.java, DictionaryWriter)

            return writer
        }
    }
}

fun DataOutput.writeIntEncoded(payload: Int, shrink: Boolean = true, markLastByteFinal: Boolean = true) {
    val endMask: Int = if (markLastByteFinal) 0b1000_0000 else 0

    if (!shrink || payload < 0 || payload > 0xFFF_FFFF) {
        writeByte((payload ushr 28) and 0b0000_1111)
        writeByte((payload ushr 21) and 0b0111_1111)
        writeByte((payload ushr 14) and 0b0111_1111)
        writeByte((payload ushr 7) and 0b0111_1111)
        writeByte((payload and 0b0111_1111) or endMask)
        return
    }

    if (payload <= 0b0111_1111) {
        writeByte(0b1000_0000 or payload)
    } else if (payload <= 0x3FFF) {
        writeByte((payload ushr 7) and 0b0111_1111)
        writeByte((payload and 0b0111_1111) or endMask)
    } else if (payload <= 0x1F_FFFF) {
        writeByte((payload ushr 14) and 0b0111_1111)
        writeByte((payload ushr 7) and 0b0111_1111)
        writeByte((payload and 0b0111_1111) or endMask)
    } else {
        writeByte((payload ushr 21) and 0b0111_1111)
        writeByte((payload ushr 14) and 0b0111_1111)
        writeByte((payload ushr 7) and 0b0111_1111)
        writeByte((payload and 0b0111_1111) or endMask)
    }
}

object LongIntegerWriter : BinaryPrologWriter.TermWriter<PrologLongInteger> {
    private val TYPE_BYTE: Int = 0x10

    override fun writeTermTo(term: PrologLongInteger, out: DataOutput, writerRef: BinaryPrologWriter) {
        out.writeByte(TYPE_BYTE)
        out.writeIntEncoded(8)
        out.writeLong(term.value)
    }
}

object BigNumberWriter : BinaryPrologWriter.TermWriter<PrologBigNumber> {
    private val TYPE_BYTE: Int = 0x11

    override fun writeTermTo(term: PrologBigNumber, out: DataOutput, writerRef: BinaryPrologWriter) {
        val (mantissa, signum, scale) = term.serialize()
        out.writeByte(TYPE_BYTE)
        out.writeByte(if (signum < 0) 0 else 1)
        out.writeLong(scale)
        out.writeIntEncoded(mantissa.size)
        out.write(mantissa)
    }
}

object VariableWriter : BinaryPrologWriter.TermWriter<Variable> {
    private val TYPE_BYTE_REGULAR = 0x20
    private val TYPE_BYTE_ANONYMOUS = 0x21

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
    private val TYPE_BYTE = 0x24

    override fun writeTermTo(term: PrologString, out: DataOutput, writerRef: BinaryPrologWriter) {
        val encoded = Charsets.UTF_8.encode(term.toKotlinString())
        out.writeByte(TYPE_BYTE)
        out.writeIntEncoded(encoded.limit())
        while (encoded.hasRemaining()) out.writeByte(encoded.get().toInt())
    }
}

object AtomWriter : BinaryPrologWriter.TermWriter<Atom> {
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

object PredicateWriter : BinaryPrologWriter.TermWriter<CompoundTerm> {
    private val TYPE_BYTE = 0x30

    override fun writeTermTo(term: CompoundTerm, out: DataOutput, writerRef: BinaryPrologWriter) {
        out.writeByte(TYPE_BYTE)
        writeWithoutTypeByteTo(term, out, writerRef)
    }

    fun writeWithoutTypeByteTo(term: CompoundTerm, out: DataOutput, writerRef: BinaryPrologWriter) {
        out.writeIntEncoded(term.arity)
        AtomWriter.writeWithoutTypeByte(term.functor, out)
        for (argument in term.arguments) {
            writerRef.writeTermTo(argument, out)
        }
    }
}

object ListWriter : BinaryPrologWriter.TermWriter<PrologList> {
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

private object QueryWriter {
    fun writeQueryTo(query: Query, out: DataOutput, writerRef: BinaryPrologWriter) {
        when (query) {
            is PredicateInvocationQuery -> writePredicateQueryTo(query, out, writerRef)
            is AndQuery -> writeCombinedQueryTo(0x00, query.goals, out, writerRef)
            is OrQuery -> writeCombinedQueryTo(0x01, query.goals, out, writerRef)
            else -> throw BinaryPrologSerializationException("Query type ${query::class.java.name} not supported by binary prolog 1.0")
        }
    }

    private fun writePredicateQueryTo(query: PredicateInvocationQuery, out: DataOutput, writerRef: BinaryPrologWriter) {
        out.writeByte(0x60)
        PredicateWriter.writeWithoutTypeByteTo(query.goal, out, writerRef)
    }

    private fun writeCombinedQueryTo(operatorByte: Int, queries: Array<out Query>, out: DataOutput, writerRef: BinaryPrologWriter) {
        out.writeByte(0x61)
        out.writeByte(operatorByte)
        out.writeIntEncoded(queries.size)
        for (subQuery in queries) {
            writeQueryTo(subQuery, out, writerRef)
        }
    }
}
