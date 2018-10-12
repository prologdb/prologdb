package com.github.prologdb.storage.heapfile

import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.lazysequence.buildLazySequence
import com.github.prologdb.storage.*
import com.github.prologdb.storage.predicate.PersistenceID
import com.github.prologdb.util.concurrency.ClearableThreadLocal
import com.github.prologdb.util.concurrency.RegionReadWriteLockManager
import com.github.prologdb.util.memory.FirstFitHeapManager
import com.github.prologdb.util.memory.HeapManager
import java.io.Closeable
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.concurrent.withLock
import kotlin.math.min

/**
 * A file o store arbitrary records, organized as a heap. Includes
 * optimizations for multithreading.
 */
/*
 Unofficial documentation/implementation internals
 * The PersistenceIDs used by this class have two components:
 * * The MSB / sign bit is always 0
 * * The upper 16 bits are the number of pages occupied by the record
 * * The lower 48 bits is the index (0-based) of the first page of the record
 */
class HeapFile
private constructor(
    /** The existing and prepared heap-file */
    private val existingFile: Path
) : Closeable, AutoCloseable
{
    /**
     * When a writing action occurs this is the probability that the class will use the opportunity
     * to instruct the OS to flush all prior changes to disk. This is slow so it is adjustable.
     *
     * E.g. if this is set to 0.001, 1 out of 1000 (on average) writing actions will cause a flush.
     */
    var flushLikelyhood: Double = 0.0001
        set(value) {
            if (value < 0.0 || value > 1.0) throw IllegalArgumentException("Must be in range [0; 1]")
            field = value
        }

    private var randomAccessFile = RandomAccessFile(existingFile.toFile(), "rw")

    /**
     * Manages read and write locks on the file. One unit in the ranges of the manager
     * corresponds to one page (as opposed to one byte).
     */
    private val readWriteLockManager = RegionReadWriteLockManager("HeapFile $existingFile")

    private val pageSize: Int
    private val offsetToFirstPage: Long
    private val maxRecordSize: Int

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
        maxRecordSize = pageSize * Short.MAX_VALUE
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
     * @param data The data of the record. Is entirely consumed if this call succeeds.
     * @param flushToDisk Whether to wait until the changes have been persisted to the physical storage device
     *                    before returning. **Attention:** this is very slow. Setting this to true on a majority
     *                    of invocations will *ruin* performance (less then 1 MiB/s write speed on a 7200rpm HDD).
     * @return A reference to be used to re-obtain the record.
     */
    fun addRecord(data: ByteBuffer, flushToDisk: Boolean = false): PersistenceID {
        if (closed) throw IOException("The heapfile is closed.")

        val recordSize = data.remaining()
        if (recordSize > maxRecordSize) {
            throw RecordTooLargeException(recordSize.toLong(), maxRecordSize.toLong())
        }

        val pagesForRecord = if (recordSize < pageSize - 5) 1 else 1 + (recordSize - (pageSize - 5) + pageSize - 1) / pageSize
        val pages = synchronized(heapManager) {
            heapManager.allocate(pagesForRecord.toLong(), true)
                ?: throw OutOfStorageMemoryException("Failed to allocate $pagesForRecord pages in the heap file $existingFile")
        }

        println("""
            {
                "action": "add",
                "recordSize": $recordSize,
                "firstPage": ${pages.first},
                "lastPage": ${pages.endInclusive}
            }
            """.trimIndent())

        try {
            val (bufferArr, bufferObj) = onePageBuffer.get()

            readWriteLockManager[pages].writeLock().withLock {
                val channel = fileChannel.get()

                var nPageInRecord = 0
                var offsetToCurrentPage = offsetToFirstPage + pages.first * pageSize
                while (data.hasRemaining()) {
                    if (nPageInRecord == 0) {
                        bufferArr[0] = PAGE_FLAGS_FIRST_RECORD_PAGE
                        // record size
                        bufferArr[1] = (recordSize ushr 24).toByte()
                        bufferArr[2] = (recordSize ushr 16).toByte()
                        bufferArr[3] = (recordSize ushr 8).toByte()
                        bufferArr[4] = recordSize.toByte()

                        // get payload data from the payload buffer
                        data.get(bufferArr, 5, min(data.remaining(), pageSize - 5))
                    } else {
                        bufferArr[0] = PAGE_FLAGS_CONTINUATION_PAGE

                        // get payload data from the payload buffer
                        data.get(bufferArr, 1, min(data.remaining(), pageSize - 1))
                    }

                    // write payload to channel
                    bufferObj.position(0)
                    bufferObj.limit(bufferArr.size)
                    channel.write(bufferObj, offsetToCurrentPage)

                    nPageInRecord++
                    offsetToCurrentPage += pageSize
                }

                if (flushToDisk || diskFlushLottery()) channel.force(false)
            }

            return toPersistenceID(pagesForRecord.toShort(), pages.first)
        }
        catch (ex: Throwable) {
            // free the page again
            synchronized(heapManager) {
                heapManager.free(pages)
            }
            throw ex
        }
    }

    /**
     * Reads the record for the given [PersistenceID] from this file and invokes the given
     * transform with the data. The byte buffer given to the transform **MUST NOT** be used in any
     * way after the transform has returned.
     *
     * @throws InvalidPersistenceIDException
    */
    fun <T> useRecord(persistenceID: PersistenceID, transform: (ByteBuffer) -> T) : T {
        if (closed) throw IOException("The heapfile is closed.")

        val (bufferArr, bufferObj) = onePageBuffer.get()
        val channel = fileChannel.get()

        // read the first page, then decide if we need more
        bufferObj.clear()
        val (nPages, firstPageIndex) = persistenceID
        val pages = LongRange(firstPageIndex, firstPageIndex + nPages)
        val firstPageOffset = offsetToFirstPage + firstPageIndex * pageSize

        readWriteLockManager[pages].readLock().withLock {
            if (channel.read(bufferObj, firstPageOffset) < 0) {
                throw InvalidPersistenceIDException(persistenceID, "Beyond EOF")
            }

            val flags = bufferArr[0]
            if (flags hasFlag PAGE_FLAG_DELETED || flags hasFlag PAGE_FLAG_CONTINUATION || !(flags hasFlag PAGE_FLAG_FIRST_PAGE_OF_RECORD)) {
                throw InvalidPersistenceIDException(persistenceID, "Record might have been overwritten since added.")
            }

            val recordSize = (bufferArr[1].toInt() and 0xFF shl 24) or (bufferArr[2].toInt() and 0xFF shl 16) or (bufferArr[3].toInt() and 0xFF shl 8) or (bufferArr[4].toInt() and 0xFF)
            if (recordSize <= pageSize - 5) {
                // all of the data is in this one page
                bufferObj.position(5) // 1 for flags, 4 for record size
                bufferObj.limit(recordSize + 5)
                return transform(bufferObj)
            } else {
                val fullRecordBuffer = ByteBuffer.allocateDirect(recordSize)

                // copy the first page
                bufferObj.position(5)
                bufferObj.limit(pageSize)
                fullRecordBuffer.put(bufferObj)

                // read the other pages
                var bytesOfRecordRemaining = recordSize - (pageSize - 5)
                var offsetToCurrentPage = firstPageOffset + pageSize // start at page 2

                while (bytesOfRecordRemaining > 0) {
                    bufferObj.clear()
                    bufferObj.limit(min(bufferObj.capacity(), bytesOfRecordRemaining + 1)) // +1 for the flag byte
                    channel.read(bufferObj, offsetToCurrentPage)

                    val pageFlags = bufferArr[0]
                    if (!(pageFlags hasFlag PAGE_FLAG_CONTINUATION)) {
                        throw StorageException("Invalid internal state - record contains non-continuation flagged page (offset of first page in record: $firstPageOffset)")
                    }

                    bufferObj.flip()
                    bufferObj.position(1) // skip the flags byte
                    bytesOfRecordRemaining -= bufferObj.remaining()
                    fullRecordBuffer.put(bufferObj)

                    offsetToCurrentPage += pageSize
                }

                fullRecordBuffer.flip()
                return transform(fullRecordBuffer)
            }
        }
    }

    /**
     * Attempts to use the record which starts at the given page index. This is
     * intended for cases where the persistenceID is not known (which contains the
     * number of pages in the record), e.g. scanning.
     *
     * @return The actual persistence ID for the record and the result of the transform.
     * @throws InvalidPersistenceIDException If the given page index is not the start of a valid record.
     */
    private fun <T> tryUseRecordStartingAtPage(firstPageIndex: Long, transform: (ByteBuffer) -> T): Pair<PersistenceID, T> {
        if (closed) throw IOException("The heapfile is closed.")

        val (bufferArr, bufferObj) = onePageBuffer.get()
        val channel = fileChannel.get()

        // read the first page, then decide if we need more
        bufferObj.clear()
        val offsetFirstPageOfRecord = offsetToFirstPage + firstPageIndex * pageSize
        readWriteLockManager[firstPageIndex..firstPageIndex].readLock().withLock {
            channel.position(offsetFirstPageOfRecord)
            channel.read(bufferObj)

            val flags = bufferArr[0]
            if (flags hasFlag PAGE_FLAG_DELETED || flags hasFlag PAGE_FLAG_CONTINUATION || !(flags hasFlag PAGE_FLAG_FIRST_PAGE_OF_RECORD)) {
                throw InvalidPersistenceIDException(firstPageIndex, "Invalid persistence ID: might have been overwritten since first storage.")
            }

            val recordSize = (bufferArr[1].toInt() and 0xFF shl 24) or (bufferArr[2].toInt() and 0xFF shl 16) or (bufferArr[3].toInt() and 0xFF shl 8) or (bufferArr[4].toInt() and 0xFF)
            if (recordSize <= pageSize - 5) {
                // all of the data is in this one page
                bufferObj.position(5) // 1 for flags, 4 for record size
                bufferObj.limit(recordSize + 5)

                val actualPersistenceID = toPersistenceID(1, firstPageIndex)

                return Pair(actualPersistenceID, transform(bufferObj))
            } else {
                val additionalPages = (recordSize - (pageSize - 5) + pageSize - 1) / pageSize
                val fullRecordBuffer = ByteBuffer.allocateDirect(recordSize)

                val actualPersistenceID = toPersistenceID((additionalPages + 1).toShort(), firstPageIndex)

                // the first page is not copied here because of a potential race condition:
                // the other pages of the record are not locked yet

                // read the other pages
                val pages = LongRange(firstPageIndex, firstPageIndex + additionalPages + 1)
                readWriteLockManager[pages].readLock().withLock {
                    val channel = fileChannel.get()
                    channel.position(offsetFirstPageOfRecord)

                    var bytesOfRecordRemaining = recordSize
                    var isFirstPage = true
                    while (bytesOfRecordRemaining > 0) {
                        bufferObj.clear()
                        bufferObj.limit(min(bufferObj.capacity(), bytesOfRecordRemaining + 1)) // +1 for the flag byte
                        channel.read(bufferObj)

                        val pageFlags = bufferArr[0]
                        if (isFirstPage && !(pageFlags hasFlag PAGE_FLAG_FIRST_PAGE_OF_RECORD)) {
                            throw InvalidPersistenceIDException(
                                actualPersistenceID,
                                "First page of record does not have the FIRST_PAGE_OF_RECORD flag.",
                                ConcurrentModificationException("The record has been modified while reading.")
                            )
                        }
                        else if (!isFirstPage && !(pageFlags hasFlag PAGE_FLAG_CONTINUATION)) {
                            throw StorageException("Invalid internal state - record contains non-continuation flagged page (offset of first page in record: $offsetFirstPageOfRecord)")
                        }

                        bufferObj.flip()
                        bufferObj.position(1) // skip the flag byte
                        if (isFirstPage) {
                            // skip the flag byte and record size
                            bufferObj.position(5)
                            isFirstPage = false
                        }

                        bytesOfRecordRemaining -= bufferObj.remaining()
                        fullRecordBuffer.put(bufferObj)
                    }
                }

                fullRecordBuffer.flip()
                return Pair(actualPersistenceID, transform(fullRecordBuffer))
            }
        }
    }

    /**
     * The lazy sequence returns from this function steps through all records in this heap file. For each
     * record it finds, it invokes the given [transform] before returning the result.
     *
     * @param transform Transforms the records; the [ByteBuffer] given to this function MUST NOT be used after
     *                  the transform function returns.
     * @return The transformed records in [Pair.second] and the corresponding persistence id in [Pair.first]
     */
    fun <T> allRecords(transform: (ByteBuffer) -> T): LazySequence<Pair<PersistenceID, T>> {
        return buildLazySequence {
            var pageIndex: Long = 0

            while (pageIndex < heapManager.size) {
                try {
                    val result = tryUseRecordStartingAtPage(pageIndex, transform)
                    yield(Pair(result.first, result.second))
                    pageIndex += result.first.nPages
                }
                catch (ex: InvalidPersistenceIDException) {
                    // that page is not valid, just move on to the next one
                    pageIndex++
                }
            }
        }
    }

    /**
     * Deletes the record with the given persistence ID from this file.
     * @param flushToDisk Whether to wait until the changes have been persisted to the physical storage device
     *                    before returning. **Attention:** this is very slow. Setting this to true on a majority
     *                    of invocations will *ruin* performance (less then 1 MiB/s write speed on a 7200rpm HDD).
     * @return Whether a record was actually removed as a result of this call.
     */
    fun removeRecord(persistenceID: PersistenceID, flushToDisk: Boolean = false): Boolean {
        if (closed) throw IOException("The heapfile is closed.")

        val (nPages, firstPageIndex) = persistenceID
        val pages = LongRange(firstPageIndex, firstPageIndex + nPages - 1)
        val firstPageOffset = offsetToFirstPage + firstPageIndex * pageSize

        val (bufferArr, bufferObj) = onePageBuffer.get()
        val channel = fileChannel.get()

        bufferObj.clear()

        println("""
            {
                "action": "remove",
                "firstPage": ${pages.first},
                "lastPage": ${pages.endInclusive}
            }
        """.trimIndent())

        readWriteLockManager[pages].readLock().withLock {
            channel.read(bufferObj, firstPageOffset)

            val flags = bufferArr[0]
            if (flags hasFlag PAGE_FLAG_CONTINUATION) {
                throw IllegalArgumentException("Record might have been overwritten since first added.")
            }
            if (flags hasFlag PAGE_FLAG_DELETED) {
                // already gone
                return false
            }

            bufferArr[0] = bufferArr[0] plusFlag PAGE_FLAG_DELETED
            bufferObj.position(0)
            bufferObj.limit(pageSize)
            readWriteLockManager[pages.first..pages.first].writeLock().withLock {
                channel.write(bufferObj, firstPageOffset)
            }
            synchronized(heapManager) {
                heapManager.free(pages)
            }

            if (flushToDisk || diskFlushLottery()) channel.force(false)

            return true
        }
    }

    /**
     * @return a random boolean where the distribution of true to false is according to [flushLikelyhood]. E.g.
     * if [flushLikelyhood] is set to 0.001 this method will return true in 1 out of 1000 invocations (on average)
     * and false on the rest.
     */
    private fun diskFlushLottery(): Boolean {
        return Math.random() <= flushLikelyhood
    }

    /** Used to synchronize the [close] operation */
    private val closingMutex = Any()

    /**
     * Almost immediately after this method has been invoked, new reading or writing actions
     * are refused with an [IOException]. This method then waits for ongoing actions to
     * complete and then returns.
     */
    override fun close() {
        if (closed) return
        synchronized(closingMutex) {
            if (closed) return

            // this blocks any new operation from starting
            closed = true
        }

        // wait for all ongoing actions to complete
        readWriteLockManager.close(RegionReadWriteLockManager.CloseMode.WAITING)

        ClearableThreadLocal.clearForAllThreads(fileChannel)
        ClearableThreadLocal.clearForAllThreads(onePageBuffer)
        randomAccessFile.close()
        readWriteLockManager.close()
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

    private inline fun toPersistenceID(nPages: Short, firstPageIndex: Long): PersistenceID {
        if (nPages <= 0) throw IllegalArgumentException()
        return (nPages.toLong() shl 48) + firstPageIndex
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
            if (Files.exists(file) && Files.size(file) > 0) {
                throw IllegalArgumentException("A non-empty file or directory already exists at path $file")
            }

            val raf = RandomAccessFile(file.toFile(), "rw")
            raf.seek(0)
            raf.writeInt(0x00000001)
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
private const val PAGE_FLAG_DELETED = 0b00_00_00_10

/** Whether this page contains continuation data from the previous page */
private const val PAGE_FLAG_CONTINUATION = 0b00_00_01_00

private const val PAGE_FLAG_FIRST_PAGE_OF_RECORD =0b00_00_00_01

/** Ready-to-go flag values for the first page in a record */
private const val PAGE_FLAGS_FIRST_RECORD_PAGE: Byte = (0 or PAGE_FLAG_FIRST_PAGE_OF_RECORD).toByte()

/** Ready-to-go flag values for a continuation page */
private const val PAGE_FLAGS_CONTINUATION_PAGE: Byte = (0 or PAGE_FLAG_CONTINUATION).toByte()

private infix fun PageFlags.plusFlag(flag: Int): PageFlags = (this.toInt() or flag).toByte()
private infix fun PageFlags.hasFlag(flag: Int): Boolean = this.toInt() and flag == flag
private infix fun PageFlags.minusFlag(flag: Int): PageFlags = (this.toInt() and (flag.inv())).toByte()

val PersistenceID.nPages: Short
    get() = ((this and -0x1000000000000L) shr 48).toShort()

val PersistenceID.firstPageIndex: Long
    get() = this and 0x0000FFFFFFFFFFFFL

operator fun PersistenceID.component1() = nPages
operator fun PersistenceID.component2() = firstPageIndex