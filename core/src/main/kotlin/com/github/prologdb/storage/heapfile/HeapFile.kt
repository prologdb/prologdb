package com.github.prologdb.storage.heapfile

import com.github.prologdb.storage.OutOfStorageMemoryException
import com.github.prologdb.storage.StorageException
import com.github.prologdb.storage.predicate.PersistenceID
import com.github.prologdb.storage.readStruct
import com.github.prologdb.storage.writeStruct
import com.github.prologdb.util.concurrency.ClearableThreadLocal
import com.github.prologdb.util.concurrency.RegionReadWriteLockManager
import com.github.prologdb.util.memory.FirstFitHeapManager
import com.github.prologdb.util.memory.HeapManager
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import kotlin.concurrent.withLock
import kotlin.math.min

/**
 * Implements a persistent predicate store by means of a heap file.
 */
/*
 Unofficial documentation/implementation internals
 * The PersistenceIDs used by this class are the page numbers in the heap file
 */
class HeapFile
private constructor(
    /** The existing and prepared heap-file */
    private val existingFile: Path
) : AutoCloseable
{
    private var randomAccessFile = RandomAccessFile(existingFile.toFile(), "rwd")
    private val readWriteLockManager = RegionReadWriteLockManager("HeapFile $existingFile")

    private val pageSize: Int
    private val offsetToFirstPage: Long

    /**
     * Manages the heap. One unit in this manager corresponds to one page (as opposed to one byte).
     */
    private val heapManager: HeapManager

    init {
        randomAccessFile.seek(0)
        val fileVersion = randomAccessFile.readInt()
        if (fileVersion != 0x00000001) {
            throw IllegalArgumentException("File version is not 0x00000001 (got 0x${fileVersion.toString(16)})")
        }

        val header = randomAccessFile.readStruct(HeapFileHeader::class)
        pageSize = header.pageSize
        offsetToFirstPage = randomAccessFile.filePointer + header.alignmentPaddingSize
        heapManager = initializeHeapManagerFromFile()
    }

    /**
     * Each thread uses one file channel. Threads can read and write to their channel arbitrarily so long
     * as they obtain the correct locks from [readWriteLockManager].
     */
    private val fileChannel = ClearableThreadLocal<FileChannel>(defaultValue = { randomAccessFile.channel }, teardown = FileChannel::close)

    /**
     * Used to buffer writes; one per thread so avoid re-allocating them all the time.
     * The buffer is a result of [ByteBuffer.wrap]
     */
    private val onePageBuffer = ClearableThreadLocal<Pair<ByteArray, ByteBuffer>>(
        defaultValue = {
            val arr = ByteArray(pageSize)
            Pair(arr, ByteBuffer.wrap(arr))
        }
    )

    /** Is set to true when closed */
    private var closed = false

    /**
     * Adds a record to this heap file.
     * @param flushToDisk whether to flush. Can be set to false when adding multiple records, then only needs to be
     *                    true for the last.
     * @return A reference to be used to re-obtain the record.
     */
    fun addRecord(data: ByteBuffer, flushToDisk: Boolean = true): PersistenceID {
        val recordSize = data.remaining()
        val pagesForRecord = if (recordSize < pageSize - 5) 1 else 1 + (recordSize - (pageSize - 5) + pageSize - 1) / pageSize
        val pages = synchronized(heapManager) {
            heapManager.allocate(pagesForRecord.toLong(), true)
                ?: throw OutOfStorageMemoryException("Failed to allocate $pagesForRecord pages in the heap file $existingFile")
        }
        val (bufferArr, bufferObj) = onePageBuffer.get()

        readWriteLockManager[pages].writeLock().withLock {
            val channel = fileChannel.get()

            channel.position(offsetToFirstPage + pages.first * pageSize)

            var firstPage = true
            while (data.hasRemaining()) {
                if (firstPage) {
                    bufferArr[0] = PAGE_FLAGS_INITIAL_PAGE_OF_RECORD
                    // record size
                    bufferArr[1] = (recordSize ushr 24).toByte()
                    bufferArr[2] = (recordSize ushr 16).toByte()
                    bufferArr[3] = (recordSize ushr  8).toByte()
                    bufferArr[4] =  recordSize         .toByte()

                    // get payload data from the payload buffer
                    data.get(bufferArr, 5, min(data.remaining(), pageSize - 5))

                    // don't repeat for the following pages
                    firstPage = false
                } else {
                    bufferArr[0] = PAGE_FLAGS_CONTINUATION_PAGE

                    // get payload data from the payload buffer
                    data.get(bufferArr, 1, min(data.remaining(), pageSize - 1))
                }

                // write payload to channel
                bufferObj.position(0)
                bufferObj.limit(bufferArr.size)
                channel.write(bufferObj)
            }

            if (flushToDisk) channel.force(false)
        }

        return pages.first
    }

    /**
     * Reads the record for the given [PersistenceID] from this file and invokes the given
     * action with the data. The byte buffer given to the action **MUST NOT** be used in any
     * way after the action has returned.
     */
    fun <T> useRecord(persistenceID: PersistenceID, action: (ByteBuffer) -> T): T {
        val firstPageOffset = offsetToFirstPage + persistenceID * pageSize
        val (bufferArr, bufferObj) = onePageBuffer.get()

        // read the first page, then decide if we need more
        readWriteLockManager[firstPageOffset..(firstPageOffset + pageSize - 1)].readLock().withLock {
            val channel = fileChannel.get()
            channel.position(firstPageOffset)

            bufferObj.clear()
                channel.read(bufferObj)
        }

        val flags = bufferArr[0]
        if (flags hasFlag PAGE_FLAG_DELETED || flags hasFlag PAGE_FLAG_CONTINUATION) {
            throw IllegalArgumentException("Invalid persistence ID given - might have been overrwritten since first storage.")
        }

        val recordSize = (bufferArr[1].toInt() and 0xFF shl 24) or (bufferArr[2].toInt() and 0xFF shl 16) or (bufferArr[3].toInt() and 0xFF shl 8) or (bufferArr[4].toInt() and 0xFF)
        if (recordSize <= pageSize - 5) {
            // all of the data is in this one page
            bufferObj.position(5) // 1 for flags, 4 for record size
            bufferObj.limit(recordSize + 5)
            return action(bufferObj)
        } else {
            val additionalPages = (recordSize - (pageSize - 5) + pageSize - 1) / pageSize
            val fullRecordBuffer = ByteBuffer.allocateDirect(recordSize)

            // copy the first page
            bufferObj.position(5)
            bufferObj.limit(pageSize)
            fullRecordBuffer.put(bufferObj)

            // read the other pages
            val secondPageOffset = firstPageOffset + pageSize
            var bytesOfRecordRemaining = recordSize - (pageSize - 5)
            readWriteLockManager[secondPageOffset..(secondPageOffset + pageSize * additionalPages)].readLock().withLock {
                val channel = fileChannel.get()

                while (bytesOfRecordRemaining > 0) {
                    bufferObj.clear()
                    channel.read(bufferObj)

                    val pageFlags = bufferArr[0]
                    if (!(pageFlags hasFlag PAGE_FLAG_CONTINUATION)) {
                        throw StorageException("Invalid internal state - record contains non-continuation flagged page (page offset $secondPageOffset")
                    }

                    bufferObj.flip()
                    bytesOfRecordRemaining -= bufferObj.remaining()
                    fullRecordBuffer.put(bufferObj)
                }
            }

            return action(fullRecordBuffer)
        }
    }

    /** Used to synchronize the [close] operation */
    private val closingMutex = Any()
    override fun close() {
        if (closed) return
        synchronized(closingMutex) {
            if (closed) return

            // this blocks any new operation from starting
            closed = true
        }

        TODO("wait for the ongoing operations to stop")
        ClearableThreadLocal.clearForAllThreads(fileChannel)
        ClearableThreadLocal.clearForAllThreads(onePageBuffer)
        randomAccessFile.close()
    }

    /**
     * Scans through the entire file and [HeapManager.allocate]s all areas that contain data. Assumes
     * exclusive access to [randomAccessFile] and [heapManager].
     */
    private fun initializeHeapManagerFromFile(): HeapManager {
        val builder = FirstFitHeapManager.fromExistingLayoutSubtractiveBuilder(1, 0.2f)

        randomAccessFile.seek(offsetToFirstPage)
        var currentPageIndex: Long = 0
        val pageBuffer = ByteArray(pageSize)

        // index of the first deleted page in the current succession
        var currentSectionStart: Long? = null
        var nPagesFound: Long = 0

        while (randomAccessFile.read(pageBuffer) == pageSize) {
            val flags = pageBuffer[0] as PageFlags
            nPagesFound++

            if (flags hasFlag PAGE_FLAG_DELETED) {
                if (currentSectionStart != null) {
                    currentSectionStart = currentPageIndex
                }
            }
            else {
                if (currentSectionStart != null) {
                    val section = currentSectionStart..currentPageIndex
                    builder.markAreaFree(section)
                    currentSectionStart = null
                }
            }
        }

        return builder.build(nPagesFound)
    }

    companion object {
        fun forExistingFile(file: Path): HeapFile {
            return HeapFile(file)
        }

        /**
         * Initializes the a file at the given path, optimized for storage devices with
         * contiguous memory (e.g. SSDs, ram disks).
         */
        fun initializeForContiguousDevice(file: Path) {
            if (Files.exists(file)) {
                throw IllegalArgumentException("A file or directory already exists at path $file")
            }

            val raf = RandomAccessFile(file.toFile(), "rw")
            raf.seek(0)
            raf.writeStruct(HeapFileHeader(1024, 0))
            raf.close()
        }

        /**
         * Initializes the file at the given path, optimized for storage devices wichs memory
         * is separated into blocks (e.g. HDD).
         */
        fun initializeForBlockDevice(file: Path) {
            return initializeForContiguousDevice(file)
        }
    }
}

/** Prepended to every page */
private typealias PageFlags = Byte

/** Whether this page is marked as deleted */
private const val PAGE_FLAG_DELETED = 0b00_00_00_01

/** Whether this page contains continuation data from the previous page */
private const val PAGE_FLAG_CONTINUATION = 0b00_00_00_10

/** Ready-to-go flag values for the first page in a record */
private const val PAGE_FLAGS_INITIAL_PAGE_OF_RECORD: Byte = 0

/** Ready-to-go flag values for a continuation page */
private const val PAGE_FLAGS_CONTINUATION_PAGE: Byte = (0 or PAGE_FLAG_CONTINUATION).toByte()

private infix fun PageFlags.plusFlag(flag: Int): PageFlags = (this.toInt() or flag).toByte()
private infix fun PageFlags.hasFlag(flag: Int): Boolean = this.toInt() and flag == flag
private infix fun PageFlags.minusFlag(flag: Int): PageFlags = (this.toInt() and (flag.inv())).toByte()