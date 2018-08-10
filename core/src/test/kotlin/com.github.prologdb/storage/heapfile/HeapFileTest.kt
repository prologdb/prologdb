package com.github.prologdb.storage.heapfile

import com.github.prologdb.storage.StorageException
import io.kotlintest.matchers.shouldThrow
import io.kotlintest.specs.FreeSpec
import java.io.File
import java.nio.ByteBuffer
import java.util.*

class HeapFileTest : FreeSpec({
    "write and read back - smaller than pagesize" {
        val tmpFile = File.createTempFile("heapfiletest", Math.random().toString())
        tmpFile.deleteOnExit()
        HeapFile.initializeForBlockDevice(tmpFile.toPath())
        val heapFile = HeapFile.forExistingFile(tmpFile.toPath()).use { heapFile ->
            val buf = bufferOfRandomValues(500)
            val pID = heapFile.addRecord(buf)

            heapFile.useRecord(pID) { dataReadBack ->
                buf.position(0)
                buf.limit(buf.capacity())
                val bufData = ByteArray(buf.capacity())
                buf.get(bufData)
                val recordData = ByteArray(dataReadBack.remaining())
                dataReadBack.get(recordData)

                assert(Arrays.equals(bufData, recordData))
            }
        }
    }

    "write and read back - larger than pagesize" {
        val tmpFile = File.createTempFile("heapfiletest", Math.random().toString())
        tmpFile.deleteOnExit()
        HeapFile.initializeForBlockDevice(tmpFile.toPath())
        HeapFile.forExistingFile(tmpFile.toPath()).use { heapFile ->
            val buf = bufferOfRandomValues(8000)
            val pID = heapFile.addRecord(buf)

            heapFile.useRecord(pID) { dataReadBack ->
                buf.position(0)
                buf.limit(buf.capacity())
                val bufData = ByteArray(buf.capacity())
                buf.get(bufData)
                val recordData = ByteArray(dataReadBack.remaining())
                dataReadBack.get(recordData)

                assert(Arrays.equals(bufData, recordData))
            }
        }
    }

    "write, delete, readback should fail" {
        val tmpFile = File.createTempFile("heapfiletest", Math.random().toString())
        tmpFile.deleteOnExit()
        HeapFile.initializeForBlockDevice(tmpFile.toPath())
        HeapFile.forExistingFile(tmpFile.toPath()).use { heapFile ->
            val buf = bufferOfRandomValues(8000)
            val pID = heapFile.addRecord(buf)

            heapFile.removeRecord(pID)

            shouldThrow<StorageException> {
                heapFile.useRecord(pID, {})
            }
        }
    }
})

private fun bufferOfRandomValues(size: Int): ByteBuffer {
    val buf = ByteArray(size)
    Random().nextBytes(buf)
    return ByteBuffer.wrap(buf)
}