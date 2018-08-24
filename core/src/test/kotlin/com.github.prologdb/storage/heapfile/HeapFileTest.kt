package com.github.prologdb.storage.heapfile

import com.github.prologdb.Performance
import com.github.prologdb.storage.StorageException
import com.github.prologdb.storage.StorageStrategy
import com.github.prologdb.storage.rootDeviceProperties
import io.kotlintest.matchers.beLessThanOrEqualTo
import io.kotlintest.matchers.should
import io.kotlintest.matchers.shouldThrow
import io.kotlintest.specs.FreeSpec
import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util.*
import kotlin.math.max
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
            val tmpFileOptimalBufferSize = tmpFile.toPath().rootDeviceProperties!!.optimalIOSize ?: 8192
            val random = Random()

            // test raw throughput
            var rawTestBytesWritten = 0
            var rawTestTimeElapsed = 0
            var startedAt: Long = 0
            FileOutputStream(tmpFile).use { fOut ->
                val buffer = ByteArray(tmpFileOptimalBufferSize)

                startedAt = System.currentTimeMillis()
                while (rawTestTimeElapsed < 5_000) {
                    random.nextBytes(buffer)
                    fOut.write(buffer)
                    rawTestBytesWritten += buffer.size
                    rawTestTimeElapsed = (System.currentTimeMillis() - startedAt).toInt()
                }
            }

            rawTestTimeElapsed = (System.currentTimeMillis() - startedAt).toInt()
            val rawThroughputMibPerSecond = (rawTestBytesWritten.toDouble() / (1024.0 * 1024.0)) / (rawTestTimeElapsed.toDouble() / 1000.0)

            // clear the file
            tmpFile.writeBytes(ByteArray(0))

            HeapFile.initializeForBlockDevice(tmpFile.toPath())
            HeapFile.forExistingFile(tmpFile.toPath()).use { heapFile ->
                var dataWritten: Long= 0
                val bufferArr = ByteArray(8192)
                random.nextBytes(bufferArr)
                val bufferObj = ByteBuffer.wrap(bufferArr)

                val startedAt = System.currentTimeMillis()
                var timeElapsedMillis = 0
                while (dataWritten < mib500 && timeElapsedMillis < 5_000) {
                    val entryMaxSize = max(512, min((mib500 - dataWritten).toInt(), bufferObj.capacity()))
                    val entrySize = random.nextInt(entryMaxSize - 1) + 1
                    bufferObj.position(0)
                    bufferObj.limit(entrySize)
                    heapFile.addRecord(bufferObj)

                    dataWritten += entrySize
                    timeElapsedMillis = (System.currentTimeMillis() - startedAt).toInt()
                }
                bufferObj.flip()
                dataWritten += bufferObj.remaining()
                heapFile.addRecord(bufferObj, true)
                timeElapsedMillis = (System.currentTimeMillis() - startedAt).toInt()

                val throughputInMibPerSecond = (dataWritten.toDouble() / (1024.0 * 1024.0)) / (timeElapsedMillis.toDouble() / 1000.0)
                val throughputRatio = rawThroughputMibPerSecond / throughputInMibPerSecond
                println("Load test result: raw throughput to disk: $rawThroughputMibPerSecond MiB/s, throughput single-threaded to heapfile: $throughputInMibPerSecond MiB/s, ratio = $throughputRatio")

                // if the heapfile is more than 4x slower than flat out writing, something is seriously wrong
                throughputRatio should beLessThanOrEqualTo(4.0)
            }
        }.config(tags = setOf(Performance))
    }
})

private fun bufferOfRandomValues(size: Int): ByteBuffer {
    val buf = ByteArray(size)
    Random().nextBytes(buf)
    return ByteBuffer.wrap(buf)
}

private fun newTmpFileOnHDD(prefix: String, minimumFreeStorage: Long): File {
    fun predicate(rootFile: File): Boolean {
        if (rootFile.freeSpace < minimumFreeStorage) {
            return false
        }

        val deviceProperties = rootFile.toPath().rootDeviceProperties ?: return false
        return deviceProperties.physicalStorageStrategy == StorageStrategy.ROTATIONAL_DISKS
    }

    val tmpDirectory = Paths.get(System.getProperty("java.io.tmpdir"))
    val root: File = if (predicate(tmpDirectory.toFile())) {
        tmpDirectory.toFile()
    } else {
        File.listRoots().firstOrNull(::predicate)
    }
        ?: throw RuntimeException("This machine does not have an HDD with at least $minimumFreeStorage bytes of free space")

    var tmpFile: File
    do {
        tmpFile = File(root, "$prefix-${System.nanoTime().toString(36)}-${(Math.random() * 100000).toInt().toString(36)}")
    } while (!tmpFile.createNewFile())
    tmpFile.deleteOnExit()

    return tmpFile
}