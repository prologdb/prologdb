package com.github.prologdb.util.memory

import java.util.SortedSet
import java.util.TreeSet
import kotlin.collections.ArrayList
import kotlin.collections.any
import kotlin.collections.first
import kotlin.collections.firstOrNull
import kotlin.collections.forEach
import kotlin.collections.lastIndex
import kotlin.math.max
import kotlin.math.min

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
    val minimumViableChunkSize: Long,

    /**
     * When [allocate] cannot find a large enough chunk it will call [defragmentFreeSpace] with this value as the
     * `effortFactor` parameter and attempt another try.
     * Must be in the range [0; 1]
     */
    private val defragEffortFactor: Float = 0.1f
) : HeapManager {
    override var size: Long = initialSize
        private set

    /**
     * The free chunks, sorted by size ascending
     */
    private val freeChunksSorted: SortedSet<LongRange> = TreeSet<LongRange>(compareBy(LongRange::size))

    /**
     * Amount of free space (essentially `freeChunksSorted.sumBy { it::size }`, but maintained
     * separately for speed when accessing).
     */
    var freeSpaceAmount: Long = size
        private set

    init {
        if (defragEffortFactor < 0f || defragEffortFactor > 1f) {
            throw IllegalArgumentException("Defragmentation effort factor must be in [0; 1]")
        }
        freeChunksSorted.add(0L .. (size - 1))
    }

    /**
     * Defragments the freed chunk list (search for separate but adjacent free chunks and combine them into one).
     * @param effortFactor In `[0; 1]`. Fraction of the number of chunks to look at. E.g. if there are 46 free chunks and this
     *                     is set to `0.2f`, will look at a maximum of `ceil(46 * 0.2 = 9.2) = 10` chunks.
     * @param abortIfCombinedOneOfAtLeast The defragmentation will be aborted as soon as a chunk of at least this
     *                     size was the result of a combination of smaller chunks. If null does not abort.
     * @return Whether fragmentation of the free chunk list was reduced as a result of the call.
     */
    fun defragmentFreeSpace(effortFactor: Float, abortIfCombinedOneOfAtLeast: Long? = null): Boolean {
        if (defragEffortFactor < 0f || defragEffortFactor > 1f) {
            throw IllegalArgumentException("Effort factor must be in [0; 1]")
        }
        if (abortIfCombinedOneOfAtLeast != null && abortIfCombinedOneOfAtLeast < 1) {
            throw IllegalArgumentException("abortIfCombinedOneOfAtLeast must be greater than 0.")
        }

        if (freeChunksSorted.size < 2) return false

        val maxNChunksToLookAt = Math.ceil((freeChunksSorted.size * effortFactor).toDouble()).toInt()
        var nChunksLookedAt = 0
        val chunksRandomAccess = ArrayList(freeChunksSorted)
        val toRemove = ArrayList<LongRange>(freeChunksSorted.size / 2)
        val toAdd = ArrayList<LongRange>(freeChunksSorted.size / 2)
        var pivotPointer = 0 // index of the next pivot within chunksRandomAccess
        var atLeastOneCombined = false // for the return value

        pivotCandidates@while (nChunksLookedAt < maxNChunksToLookAt) {
            val pivot = chunksRandomAccess[pivotPointer]
            nChunksLookedAt++
            findForPivot@for (nextIndex in 0..chunksRandomAccess.lastIndex) {
                if (nChunksLookedAt > maxNChunksToLookAt) break

                if (nextIndex == pivotPointer) continue

                val next = chunksRandomAccess[nextIndex]
                nChunksLookedAt++

                if (pivot.last + 1 == next.first || next.last + 1 == pivot.first) {
                    chunksRandomAccess.remove(pivot)
                    chunksRandomAccess.remove(next)
                    toRemove.add(pivot)
                    toRemove.add(next)

                    val combined = min(pivot.first, next.first)..max(pivot.last, next.last)
                    chunksRandomAccess.add(combined)
                    toAdd.add(combined)

                    atLeastOneCombined = true

                    if (abortIfCombinedOneOfAtLeast != null && combined.size >= abortIfCombinedOneOfAtLeast) {
                        break@pivotCandidates
                    }

                    pivotPointer = 0
                    break@findForPivot
                }
            }
            pivotPointer++
        }

        toRemove.forEach { freeChunksSorted.remove(it) }
        toAdd.forEach { freeChunksSorted.add(it) }

        return atLeastOneCombined
    }

    override fun allocate(desiredSize: Long, enlargeIfNecessary: Boolean): LongRange? {
        if (freeSpaceAmount < desiredSize) {
            if (!enlargeIfNecessary) return null // there is nothing we can do, out of memory
            val newChunk = size..(size + desiredSize - 1)
            size += desiredSize
            return newChunk
        }

        val chunk = freeChunksSorted.firstOrNull { it.size >= desiredSize }
            // defrag is it is likely to help
            ?: if (defragEffortFactor > 0f && desiredSize / defragEffortFactor <= freeSpaceAmount) {
                    if (defragmentFreeSpace(defragEffortFactor, desiredSize)) {
                        freeChunksSorted.first { it.size >= desiredSize }
                    } else null
                } else null
            ?: if (enlargeIfNecessary) {
                    val newChunk = size..(size + desiredSize - 1)
                    size += desiredSize
                    newChunk
                } else null

        chunk ?: return null

        freeChunksSorted.remove(chunk)

        val chunkOversize = chunk.size - desiredSize
        val allocated: LongRange
        if (chunkOversize > minimumViableChunkSize) {
            allocated = chunk.first..(chunk.first + desiredSize - 1)
            val remainder = (allocated.last + 1)..chunk.last
            freeChunksSorted.add(remainder)
        }
        else {
            allocated = chunk
        }
        freeSpaceAmount -= allocated.size
        return allocated
    }

    override fun free(chunk: LongRange) {
        if (!chunk.isAscending) throw IllegalArgumentException("Chunk must be ascending.")
        if (chunk.start < 0 || chunk.last < 0) throw IllegalArgumentException("Chunk start and end must be positive.")
        if (chunk.last > size) throw IllegalArgumentException("Cannot free space outside of this heap.")

        if (freeChunksSorted.any { it.overlapsWith(chunk) }) {
            throw IllegalArgumentException("The given range was not entirely occupied prior to this call.")
        }

        freeChunksSorted.add(chunk)
        freeSpaceAmount += chunk.size
    }
}

private infix fun LongRange.overlapsWith(other: LongRange): Boolean {
    // thanks to https://stackoverflow.com/questions/3269434/whats-the-most-efficient-way-to-test-two-integer-ranges-for-overlap/3269471

    return this.first <= other.last && other.first <= this.last
}

private val LongRange.isAscending
    get() = last >= first

private val LongRange.size
    get() = last - first + 1