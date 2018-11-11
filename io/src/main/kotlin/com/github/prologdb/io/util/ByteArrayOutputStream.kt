package com.github.prologdb.io.util

import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.*

/**
 * A special version of javas ByteArrayOutputStream. It adds these features:
 * * it keeps a [ByteBuffer] that wraps the backing [ByteArray]
 *
 * A lot of the code in this file has been copied from the JDK 8 implementation.
 */
class ByteArrayOutputStream(
    initialCapacity: Int = 32
) : OutputStream() {

    init {
        if (initialCapacity < 0) {
            throw IllegalArgumentException("Negative initial capacity")
        }
    }

    private var data: ByteArray = ByteArray(initialCapacity)
        set(value) {
            field = value
            bufferOfData = ByteBuffer.wrap(field)
        }

    /**
     * A [ByteBuffer] that always points to the internal data of this output stream.
     * Calls to [write] and [reset] will modify the [ByteBuffer.limit] of this.
     */
    var bufferOfData: ByteBuffer = ByteBuffer.wrap(data)
        private set

    /**
     * The current number of valid bytes in this buffer
     */
    private var count = 0
        set(value) {
            field = value
            bufferOfData.limit(value)
        }

    private fun grow(minCapacity: Int) {
        // overflow-conscious code
        val oldCapacity = data.size
        var newCapacity = oldCapacity shl 1
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity)
        data = Arrays.copyOf(data, newCapacity)
    }

    private fun hugeCapacity(minCapacity: Int): Int {
        if (minCapacity < 0)
        // overflow
            throw OutOfMemoryError()
        return if (minCapacity > MAX_ARRAY_SIZE)
            Integer.MAX_VALUE
        else
            MAX_ARRAY_SIZE
    }

    private fun ensureCapacity(minCapacity: Int) {
        // overflow-conscious code
        if (minCapacity - data.size > 0) grow(minCapacity)
    }

    override fun write(b: Int) {
        ensureCapacity(count + 1)
        data[count] = b.toByte()
        count++
    }

    override fun write(b: ByteArray?, off: Int, len: Int) {
        b!!
        if (off < 0 || off > b.size || len < 0 ||
            off + len - b.size > 0) {
            throw IndexOutOfBoundsException()
        }
        ensureCapacity(count + len)
        System.arraycopy(b, off, data, count, len)
        count += len
    }

    override fun close() {

    }

    /**
     * Resets the internal pointer so that previously used capacity can be re-used.
     */
    fun reset() {
        count = 0
        bufferOfData.position(0)
    }
}

/**
 * The maximum size of array to allocate.
 * Some VMs reserve some header words in an array.
 * Attempts to allocate larger arrays may result in
 * OutOfMemoryError: Requested array size exceeds VM limit
 */
private const val MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8