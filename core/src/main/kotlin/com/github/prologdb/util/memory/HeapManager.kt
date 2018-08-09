package com.github.prologdb.util.memory

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

internal infix fun LongRange.overlapsWith(other: LongRange): Boolean {
    // thanks to https://stackoverflow.com/questions/3269434/whats-the-most-efficient-way-to-test-two-integer-ranges-for-overlap/3269471

    return this.first <= other.last && other.first <= this.last
}

internal val LongRange.isAscending
    get() = last >= first

internal val LongRange.size
    get() = last - first + 1