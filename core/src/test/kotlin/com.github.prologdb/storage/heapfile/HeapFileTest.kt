package com.github.prologdb.storage.heapfile

import com.github.prologdb.Performance
import com.github.prologdb.async.IrrelevantPrincipal
import com.github.prologdb.async.forEachRemaining
import com.github.prologdb.storage.InvalidPersistenceIDException
import com.github.prologdb.storage.StorageStrategy
import com.github.prologdb.storage.fact.PersistenceID
import com.github.prologdb.storage.rootDeviceProperties
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.comparables.beLessThanOrEqualTo
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Assertions.assertFalse
import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util.Arrays
import java.util.Random
import java.util.UUID
import java.util.concurrent.CompletableFuture
import kotlin.math.max
import kotlin.math.min

class HeapFileTest : FreeSpec({
    "write and read back - smaller than pagesize" {
        val tmpFile = File.createTempFile("heapfiletest", Math.random().toString())
        tmpFile.deleteOnExit()
        HeapFile.initializeForBlockDevice(tmpFile.toPath())
        HeapFile.forExistingFile(tmpFile.toPath()).use { heapFile ->
            val buf = bufferOfRandomValues(500)
            val pID = heapFile.addRecord(IrrelevantPrincipal, buf).get()

            heapFile.useRecord(IrrelevantPrincipal, pID) { dataReadBack ->
                buf.position(0)
                buf.limit(buf.capacity())
                val bufData = ByteArray(buf.capacity())
                buf.get(bufData)
                val recordData = ByteArray(dataReadBack.remaining())
                dataReadBack.get(recordData)

                assert(Arrays.equals(bufData, recordData))
            }.get()
        }
    }

    "write and read back - larger than pagesize" {
        val tmpFile = File.createTempFile("heapfiletest", Math.random().toString())
        tmpFile.deleteOnExit()
        HeapFile.initializeForBlockDevice(tmpFile.toPath())
        HeapFile.forExistingFile(tmpFile.toPath()).use { heapFile ->
            val buf = bufferOfRandomValues(8000)
            val pID = heapFile.addRecord(IrrelevantPrincipal, buf).get()

            heapFile.useRecord(IrrelevantPrincipal, pID) { dataReadBack ->
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
            val pID = heapFile.addRecord(IrrelevantPrincipal, buf).get()

            heapFile.removeRecord(IrrelevantPrincipal, pID).get()

            shouldThrow<InvalidPersistenceIDException> {
                heapFile.useRecord(IrrelevantPrincipal, pID, {
                    val x = it
                }).get()
            }
        }
    }

    "f:multi thread write&read" {
        val tmpFile = File.createTempFile("heapfiletest", Math.random().toString())
        tmpFile.deleteOnExit()

        HeapFile.initializeForBlockDevice(tmpFile.toPath())
        val hf = HeapFile.forExistingFile(tmpFile.toPath())

        // this test is empirically unreliable when run once
        for (i in 0..100) {
            val taObjWritten = CompletableFuture<Pair<PersistenceID, ByteArray>>()
            val tA = testingThread {
                val principal = UUID.randomUUID()
                val random = Random()
                val bufferArr = ByteArray(1500)
                val bufferObj = ByteBuffer.wrap(bufferArr)

                for (i in 0..3) {
                    bufferObj.clear()
                    random.nextBytes(bufferArr)
                    hf.addRecord(principal, bufferObj).get()
                }
                bufferObj.clear()
                random.nextBytes(bufferArr)
                val pID = hf.addRecord(principal, bufferObj).get()
                val controlData = ByteArray(bufferArr.size, bufferArr::get)
                taObjWritten.complete(Pair(pID, controlData))

                for (i in 0..3) {
                    bufferObj.clear()
                    random.nextBytes(bufferArr)
                    hf.addRecord(principal, bufferObj).get()
                }
            }

            val tB = testingThread {
                val principal = UUID.randomUUID()
                val random = Random()
                val bufferArr = ByteArray(1800)
                val bufferObj = ByteBuffer.wrap(bufferArr)

                bufferObj.clear()
                random.nextBytes(bufferArr)
                hf.addRecord(principal, bufferObj).get()
                taObjWritten.join()

                hf.useRecord(principal, taObjWritten.get().first) { recordData ->
                    val recordDataArray = ByteArray(recordData.remaining())
                    recordData.get(recordDataArray)

                    assert(Arrays.equals(taObjWritten.get().second, recordDataArray))
                }
            }

            tA.join()
            tB.join()
            tA.propagateUncaughtExceptionIfPresent()
            tB.propagateUncaughtExceptionIfPresent()
        }
    }

    "scan" {
        val tmpFile = File.createTempFile("heapfiletest", Math.random().toString())
        tmpFile.deleteOnExit()
        HeapFile.initializeForBlockDevice(tmpFile.toPath())
        HeapFile.forExistingFile(tmpFile.toPath()).use { heapFile ->
            val writtenBuffers = mutableMapOf<PersistenceID, ByteBuffer>()

            for (n in 0..50) {
                val buf = bufferOfRandomValues(768)
                val pID = heapFile.addRecord(IrrelevantPrincipal, buf).get()

                assertFalse(pID in writtenBuffers)
                writtenBuffers[pID] = buf
            }

            heapFile.allRecords(IrrelevantPrincipal) { data ->
                val dataCopy = ByteArray(data.remaining())
                data.get(dataCopy)
                dataCopy
            }.forEachRemaining { (pID, dataAsByteArray) ->
                val originalData = writtenBuffers[pID]!!
                originalData.flip()
                ByteBuffer.wrap(dataAsByteArray) shouldBe originalData
            }
        }
    }

    "loadtest" - {
        "single thread writes".config(tags = setOf(Performance)) {
            // disabled: heapfile page size has been reduced to 256 bytes, which make the performance go to crap
            // this should be enabled again when HeapFile has gotten the feature to do I/O in chunk sizes suitable
            // for the underlying device.

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
                    heapFile.addRecord(IrrelevantPrincipal, bufferObj).get()

                    dataWritten += entrySize
                    timeElapsedMillis = (System.currentTimeMillis() - startedAt).toInt()
                }
                bufferObj.flip()
                dataWritten += bufferObj.remaining()
                heapFile.addRecord(IrrelevantPrincipal, bufferObj, true).get()
                timeElapsedMillis = (System.currentTimeMillis() - startedAt).toInt()

                val throughputInMibPerSecond = (dataWritten.toDouble() / (1024.0 * 1024.0)) / (timeElapsedMillis.toDouble() / 1000.0)
                val throughputRatio = rawThroughputMibPerSecond / throughputInMibPerSecond
                println("Load test result: raw throughput to disk: $rawThroughputMibPerSecond MiB/s, throughput single-threaded to heapfile: $throughputInMibPerSecond MiB/s, ratio = $throughputRatio")

                // if the heapfile is more than 4x slower than flat out writing, something is seriously wrong
                throughputRatio should beLessThanOrEqualTo(4.0)
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

private fun testingThread(code: () -> Unit): TestingThread {
    val thread = TestingThread(code)
    thread.start()
    return thread
}

private class TestingThread(code: () -> Unit) : Thread(code) {
    private var uncaughtEx: Throwable? = null
    init {
        setUncaughtExceptionHandler { t, e ->
            uncaughtEx = e
        }
    }

    fun propagateUncaughtExceptionIfPresent() {
        if (uncaughtEx != null) {
            throw uncaughtEx!!
        }
    }
}