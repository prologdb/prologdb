package com.github.prologdb.storage.heapfile

import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.storage.readStruct
import com.github.prologdb.storage.writeStruct
import java.io.RandomAccessFile
import java.nio.file.Files
import java.nio.file.Path

/**
 * Implements a persistent predicate store by means of a heap file.
 */
class HeapFile
private constructor(
    /** The existing and prepared heap-file */
    private val existingFile: Path
) : AutoCloseable
{
    private var randomAccessFile = RandomAccessFile(existingFile.toFile(), "rwd")
    private val pageSize: Int
    private val offsetToFirstPage: Long

    /** Is set to true when closed */
    private var closed = false

    init {
        randomAccessFile.seek(0)
        val fileVersion = randomAccessFile.readInt()
        if (fileVersion != 0x00000001) {
            throw IllegalArgumentException("File version is not 0x00000001 (got 0x${fileVersion.toString(16)})")
        }

        val header = randomAccessFile.readStruct(HeapFileHeader::class)
        pageSize = header.pageSize
        offsetToFirstPage = randomAccessFile.filePointer + header.alignmentPaddingSize
    }

    /** Amount of data that fits onto one page, in bytes */
    private val dataPerPage = pageSize - 1 // -1 byte for the flags

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
        randomAccessFile.close()
    }

    companion object {
        fun forExistingFile(file: Path, indicator: PredicateIndicator): HeapFile {
            return forExistingFile(file, indicator)
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
private const val PAGE_FLAG_DELETED = 0b00000001

/** Whether this page contains continuation data from the previous page */
private const val PAGE_FLAG_CONTINUATION = 0b00000001

private infix fun PageFlags.plusFlag(flag: Int): PageFlags = (this.toInt() or flag).toByte()
private infix fun PageFlags.hasFlag(flag: Int): Boolean = this.toInt() and flag == flag
private infix fun PageFlags.minusFlag(flag: Int): PageFlags = (this.toInt() and (flag.inv())).toByte()