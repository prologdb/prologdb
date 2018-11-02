package com.github.prologdb.storage.predicate.heapfile

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.Principal
import com.github.prologdb.async.launchWorkableFuture
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.io.util.ByteArrayOutputStream
import com.github.prologdb.io.util.Pool
import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.storage.InvalidPersistenceIDException
import com.github.prologdb.storage.heapfile.HeapFile
import com.github.prologdb.storage.predicate.PersistenceID
import com.github.prologdb.storage.predicate.PredicateStore
import java.io.DataOutput
import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.util.concurrent.Future

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
    private val bufferOutStream: Pool<Pair<ByteArrayOutputStream, DataOutput>> = Pool(15,
        initializer = {
            val buffer = ByteArrayOutputStream(1024)
            val out = DataOutputStream(buffer)
            Pair(buffer, out as DataOutput)
        },
        sanitizer = {
            it.first.reset()
        }
    )

    override fun store(asPrincipal: Principal, item: Predicate): Future<PersistenceID> {
        if (item.arity != indicator.arity || item.name != indicator.name) {
            throw IllegalArgumentException("This predicate store is intended for instances of $indicator, got ${item.name}/${item.arity}")
        }

        return launchWorkableFuture(asPrincipal) {
            val bufferHolder = bufferOutStream.get()
            finally {
                bufferOutStream.free(bufferHolder)
            }
            val (buffer, dataOut) = bufferHolder

            for (argument in item.arguments) binaryWriter.writeTermTo(argument, dataOut)
            val byteBuffer = buffer.bufferOfData
            byteBuffer.position(0)
            return@launchWorkableFuture await(heapFile.addRecord(asPrincipal, byteBuffer))
        }
    }

    override fun retrieve(asPrincipal: Principal, id: PersistenceID): Future<Predicate?> {
        return launchWorkableFuture(asPrincipal) {
            return@launchWorkableFuture try {
                await(heapFile.useRecord(asPrincipal, id, this@HeapfilePredicateStore::readPredicateFrom))
            }
            catch (ex: InvalidPersistenceIDException) { null }
        }
    }

    override fun delete(asPrincipal: Principal, id: PersistenceID): Future<Boolean> {
        return launchWorkableFuture(asPrincipal) {
            return@launchWorkableFuture  try {
                await(heapFile.removeRecord(asPrincipal, id))
            }
            catch (ex: InvalidPersistenceIDException) { false }
        }
    }

    override fun all(asPrincipal: Principal): LazySequence<Pair<PersistenceID, Predicate>> {
        return heapFile.allRecords(asPrincipal, this::readPredicateFrom)
    }

    private fun readPredicateFrom(data: ByteBuffer): Predicate {
        val arguments = Array(indicator.arity) { binaryReader.readTermFrom(data) }
        return Predicate(indicator.name, arguments)
    }
}