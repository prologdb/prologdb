package com.github.prologdb.storage.heapfile

import com.github.prologdb.storage.StorageException
import com.github.prologdb.storage.StorageStrategy
import com.github.prologdb.storage.rootDeviceProperties
import io.kotlintest.matchers.shouldThrow
import io.kotlintest.specs.FreeSpec
import java.io.File
import java.nio.ByteBuffer
import java.util.*
import javax.swing.filechooser.FileSystemView
import kotlin.math.min

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

    "loadtest" - {
        "single thread writes" {
            val mib500 =  1024L * 1024L * 500L
            val tmpFile = newTmpFileOnHDD("heapfile-loadtest", mib500 + 8096)
            val random = Random()
            HeapFile.initializeForBlockDevice(tmpFile.toPath())
            HeapFile.forExistingFile(tmpFile.toPath()).use { heapFile ->
                var dataWritten: Long= 0
                val bufferArr = ByteArray(8912)
                random.nextBytes(bufferArr)
                val bufferObj = ByteBuffer.wrap(bufferArr)

                val startedAt = System.currentTimeMillis()
                var timeElapsedMillis = 0
                while (dataWritten < mib500 && timeElapsedMillis < 10_000) {
                    val entryMaxSize = min((mib500 - dataWritten).toInt(), bufferObj.capacity())
                    val entrySize = random.nextInt(entryMaxSize - 1) + 1
                    bufferObj.position(0)
                    bufferObj.limit(entrySize)
                    heapFile.addRecord(bufferObj)

                    dataWritten += entrySize
                    timeElapsedMillis = (System.currentTimeMillis() - startedAt).toInt()
                }

                val throughputInMibPerSecond = (dataWritten.toDouble() / (1024.0 * 1024.0)) / (timeElapsedMillis.toDouble() / 1000.0)
                println("Load test result: throughput single-threaded: $throughputInMibPerSecond MiB/s")
            }
        }
    }
})

private fun bufferOfRandomValues(size: Int): ByteBuffer {
    val buf = ByteArray(size)
    Random().nextBytes(buf)
    return ByteBuffer.wrap(buf)
}

private fun newTmpFileOnHDD(prefix: String, minimumFreeStorage: Long): File {
    val root: File = File.listRoots()
        .filter { rootFile -> rootFile.freeSpace >= minimumFreeStorage }
        .firstOrNull { rootFile -> rootFile.toPath().rootDeviceProperties.physicalStorageStrategy == StorageStrategy.ROTATIONAL_DISKS }
        ?: throw RuntimeException("This machine does not have an HDD with at least $minimumFreeStorage bytes of free space")

    var tmpFile: File
    do {
        tmpFile = File(root, "$prefix-${System.nanoTime().toString(36)}-${(Math.random() * 100000).toInt().toString(36)}")
    } while (!tmpFile.createNewFile())
    tmpFile.deleteOnExit()

    return tmpFile
}