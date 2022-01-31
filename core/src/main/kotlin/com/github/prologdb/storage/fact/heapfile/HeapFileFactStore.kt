package com.github.prologdb.storage.fact.heapfile

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.Principal
import com.github.prologdb.async.launchWorkableFuture
import com.github.prologdb.dbms.DataDirectoryManager
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.io.util.ByteArrayOutputStream
import com.github.prologdb.io.util.Pool
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.storage.InvalidPersistenceIDException
import com.github.prologdb.storage.PersistentStorage
import com.github.prologdb.storage.StorageStrategy
import com.github.prologdb.storage.fact.FactStore
import com.github.prologdb.storage.fact.PersistenceID
import com.github.prologdb.storage.fact.SpecializedFactStoreLoader
import com.github.prologdb.storage.heapfile.HeapFile
import com.github.prologdb.storage.rootDeviceProperties
import java.io.DataOutput
import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.Future

/**
 * An implementation of [FactStore] based on [HeapFile]
 */
@PersistentStorage
class HeapFileFactStore(
    private val arity: Int,
    private val binaryReader: BinaryPrologReader,
    private val binaryWriter: BinaryPrologWriter,
    private val heapFile: HeapFile
) : FactStore {

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

    override fun store(asPrincipal: Principal, arguments: Array<out Term>): Future<PersistenceID> {
        if (arguments.size != arity) {
            throw IllegalArgumentException("This store requires arity=$arity, got ${arguments.size} arguments.")
        }

        return launchWorkableFuture(asPrincipal) {
            val bufferHolder = bufferOutStream.get()
            finally {
                bufferOutStream.free(bufferHolder)
            }
            val (buffer, dataOut) = bufferHolder

            for (argument in arguments) binaryWriter.writeTermTo(argument, dataOut)
            val byteBuffer = buffer.bufferOfData
            byteBuffer.position(0)
            return@launchWorkableFuture await(heapFile.addRecord(asPrincipal, byteBuffer))
        }
    }

    override fun retrieve(asPrincipal: Principal, id: PersistenceID): Future<Array<out Term>?> {
        return launchWorkableFuture(asPrincipal) {
            return@launchWorkableFuture try {
                await(heapFile.useRecord(asPrincipal, id, this@HeapFileFactStore::readPredicateFrom))
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

    override fun all(asPrincipal: Principal): LazySequence<Pair<PersistenceID, Array<out Term>>> {
        return heapFile.allRecords(asPrincipal, this::readPredicateFrom)
    }

    private fun readPredicateFrom(data: ByteBuffer): Array<out Term> {
        return Array(arity) { binaryReader.readTermFrom(data) }
    }

    object Loader : SpecializedFactStoreLoader<HeapFileFactStore> {
        override val type = HeapFileFactStore::class

        override fun createOrLoad(directoryManager: DataDirectoryManager.PredicateScope): HeapFileFactStore {
            val path = directoryManager.directory.resolve("facts.heap")

            if (Files.notExists(path)) {
                val deviceProperties = path.rootDeviceProperties
                when (deviceProperties?.physicalStorageStrategy) {
                    StorageStrategy.SOLID_STATE -> HeapFile.initializeForContiguousDevice(path)
                    else ->                        HeapFile.initializeForBlockDevice(path)
                }
            }

            return HeapFileFactStore(
                directoryManager.catalogEntry.arity,
                BinaryPrologReader.getDefaultInstance(),
                BinaryPrologWriter.getDefaultInstance(),
                HeapFile.forExistingFile(path)
            )
        }
    }

    override fun close() {
        heapFile.close()
        bufferOutStream.clear()
    }
}
