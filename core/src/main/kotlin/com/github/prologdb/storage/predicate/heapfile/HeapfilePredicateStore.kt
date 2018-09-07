package com.github.prologdb.storage.predicate.heapfile

import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.storage.InvalidPersistenceIDException
import com.github.prologdb.storage.heapfile.HeapFile
import com.github.prologdb.storage.predicate.PersistenceID
import com.github.prologdb.storage.predicate.PredicateStore
import com.github.prologdb.util.ByteArrayOutputStream
import java.io.DataOutput
import java.io.DataOutputStream
import java.nio.ByteBuffer

/**
 * An implementation of [PredicateStore] based on [HeapFile]
 */
class HeapfilePredicateStore(
    override val indicator: PredicateIndicator,
    private val binaryReader: BinaryPrologReader,
    private val binaryWriter: BinaryPrologWriter,
    private val heapFile: HeapFile
) : PredicateStore {

    /**
     * Used to buffer binary writes of unknown size.
     *
     * The [ByteArrayOutputStream] is the actual backing storage; the [DataOutput] just points
     * to that output stream.
     */
    private val bufferOutStream: ThreadLocal<Pair<ByteArrayOutputStream, DataOutput>> = ThreadLocal.withInitial {
        val buffer = ByteArrayOutputStream(1024)
        val out = DataOutputStream(buffer)
        Pair(buffer, out as DataOutput)
    }

    override fun store(item: Predicate): PersistenceID {
        if (item.arity != indicator.arity || item.name != indicator.name) {
            throw IllegalArgumentException("This predicate store is intended for instances of $indicator, got ${item.name}/${item.arity}")
        }

        val (buffer, dataOut) = bufferOutStream.get()
        buffer.reset()

        for (argument in item.arguments) binaryWriter.writeTermTo(argument, dataOut)

        val byteBuffer = buffer.bufferOfData
        byteBuffer.position(0)
        return heapFile.addRecord(byteBuffer)
    }

    override fun retrieve(id: PersistenceID): Predicate? {
        return try {
            heapFile.useRecord(id, this::readPredicateFrom)
        }
        catch (ex: InvalidPersistenceIDException) {
            null
        }
    }

    override fun delete(id: PersistenceID): Boolean {
        return heapFile.removeRecord(id)
    }

    override fun all(): LazySequence<Pair<PersistenceID, Predicate>> {
        return heapFile.allRecords(this::readPredicateFrom)
    }

    private fun readPredicateFrom(data: ByteBuffer): Predicate {
        val arguments = Array(indicator.arity) { binaryReader.readTermFrom(data) }
        return Predicate(indicator.name, arguments)
    }
}