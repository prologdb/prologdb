package com.github.prologdb.util.memory

import java.util.SortedSet
import java.util.TreeSet

/**
 * Manager for a heap data structure. Is not associated to the actual resources; is
 * intended to be used by the owner of a heap-structured resource to outsource the
 * management.
 *
 * This class is **NOT THREAD SAFE!**
 */
interface HeapManager {
    /** Total size of the heap */
    val size: Long

    /**
     * Allocates a contiguous chunk of at least the given size.
     * @param enlargeIfNecessary If there is no chunk of the required size available, the heap could be enlarged (more memory
     * appended to the end). If this parameter is true, the manager will assume it can enlarge the heap as necessary. After
     * this method returns and if enlargement was necessary, the new size is reflected by the [size] field.
     * @return the start and end offset of the allocated range.
     */
    fun allocate(desiredSize: Long, enlargeIfNecessary: Boolean = false): LongRange?

    /**
     * Frees the given range. If the given range is not entirely allocated when this method is called an
     * exception will be thrown.
     * @param chunk The start end end offset of the chunk to free.
     */
    fun free(chunk: LongRange)
}

/**
 * A [HeapManager] using the first-fit strategy.
 */
class FirstFitHeapManager(
    initialSize: Long,

    /**
     * The minimal chunk size that makes sense. E.g. if allocating and the smallest sufficiently sized free
     * chunk is larger than requested, it might make sense to split that chunk and allocate a perfect-fit chunk.
     * That splitting will not occur when the resulting unused free chunk is smaller than this value. E.g.
     * This is set to 2048. An allocation request of size 1752 comes in and the smallest fitting chunk is
     * of size 3096. The remaining free chunk would be 3096-1752 = 1344 < 2048. Hence, the chunk will not be split.
     * If, however, the smallest fitting chunk is of size 8192, it would be split into one of size 1752 and
     * one of 6440.
     */
    val minimumViableChunkSize: Long
) : HeapManager {
    override var size: Long = initialSize
        private set

    private val freeChunksSorted: SortedSet<LongRange> = TreeSet<LongRange>(compareBy(LongRange::size))

    init {
        freeChunksSorted.add(0L .. size)
    }

    override fun allocate(desiredSize: Long, enlargeIfNecessary: Boolean): LongRange? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun free(chunk: LongRange) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

private val LongRange.isAscending
    get() = last >= first

private val LongRange.size
    get() = last - first + 1