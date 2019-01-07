package com.github.prologdb.net.async

import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.InvalidProtocolBufferException
import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.ClosedChannelException
import java.nio.channels.CompletionHandler
import java.util.concurrent.Callable
import java.util.function.Consumer
import kotlin.experimental.and
import kotlin.math.max

/**
 * Reads delimited protobuf objects from an [AsynchronousByteChannel]. For each message received,
 * invokes `onNextMessage`. If reading results in an error, invokes `onError` and stops reading
 * from the channel. When the channels EOF is reached, invokes `onChannelEnd`.
 */
class AsyncByteChannelDelimitedProtobufReader<T : GeneratedMessageV3>(
    private val typeClass: Class<T>,
    private val channel: AsynchronousByteChannel,
    private val onNextMessage: Consumer<T>,
    private val onError: Consumer<Throwable>,
    private val onChannelEnd: Callable<*>
) {
    init {
        if (typeClass == GeneratedMessageV3::class.java) {
            throw IllegalArgumentException("\$typeClass must be a subclass of ${GeneratedMessageV3::class.java.name}")
        }
    }

    private val buffer = ByteBuffer.allocateDirect(4096)

    /**
     * Used fore messages too large for [buffer]
     */
    private var currentMessageLength: Int = -1
    private var currentWholeMessageBuffer: ByteBuffer? = null

    private val MODE_LENGTH = true
    private val MODE_MESSAGE = false
    private var mode = MODE_LENGTH

    // for reading the length
    private var carry: Long = 0
    private var digitSignificance: Long = 1

    private fun readAndContinue() {
        when (mode) {
            MODE_LENGTH -> readAndContinueLength()
            MODE_MESSAGE -> readAndContinueMessage()
        }
    }

    private fun readAndContinueLength() {
        while (buffer.hasRemaining()) {
            val byte = buffer.get()
            val value = if (byte >= 0) byte else byte and 0b01111111

            carry += value.toLong() * digitSignificance

            if (carry and 0xFFFFFFFFL != carry) {
                // larger than 32 bits
                onError.accept(IOException("Variable uint larger than 32 bits!"))
                return
            }

            if (byte >= 0) {
                // last
                mode = MODE_MESSAGE
                currentMessageLength = carry.toInt()
                carry = 0
                digitSignificance = 1
                readAndContinueMessage(firstInvocationAfterLength = true)
                return
            }

            digitSignificance *= 128L
        }

        // not done yet, otherwise this code wouldn't execute
        buffer.clear()

        channel.read(buffer, null, handler)
    }

    private fun readAndContinueMessage(firstInvocationAfterLength: Boolean = false) {
        if (firstInvocationAfterLength) {
            val amountAlreadyPresent = buffer.remaining()
            if (amountAlreadyPresent >= currentMessageLength) {
                try {
                    onNextMessage.accept(createMessage(buffer, currentMessageLength))
                }
                catch (ex: Throwable) {
                    onError.accept(ex)
                }

                maximizeBufferRemaining()

                mode = MODE_LENGTH
                readAndContinueLength()
                return
            }
            else {
                initCurrentWholeMessageBuffer()
                buffer.clear()
                channel.read(buffer, null, handler)
                return
            }
        }
        // else:
        val wholeMessageBuffer = currentWholeMessageBuffer!!
        val neededForMessage = wholeMessageBuffer.remaining()
        if (neededForMessage <= buffer.remaining()) {

            // copy from buffer, only as much as needed
            val bufferLimitBefore = buffer.limit()
            buffer.limit(buffer.position() + neededForMessage)
            wholeMessageBuffer.put(buffer)
            buffer.limit(bufferLimitBefore)

            // publish message
            wholeMessageBuffer.position(0)
            onNextMessage.accept(createMessage(wholeMessageBuffer, currentMessageLength))

            wholeMessageBuffer.clear()
            maximizeBufferRemaining()

            // plough ahead
            mode = MODE_LENGTH
            readAndContinueLength()
        }
        else
        {
            // still not enough.... first consume all there is in buffer
            wholeMessageBuffer.put(buffer)

            // read more
            buffer.clear()
            channel.read(buffer, null, handler)
            return
        }
    }

    /**
     * Assures that
     * * [currentWholeMessageBuffer] has at least the capacity [currentMessageLength]
     * * it's limit is [currentMessageLength]
     * * data still remaining in [buffer] is copied over
     */
    private fun initCurrentWholeMessageBuffer() {
        if (currentWholeMessageBuffer == null || currentWholeMessageBuffer!!.capacity() < currentMessageLength) {
            currentWholeMessageBuffer = ByteBuffer.allocate(currentMessageLength)
        } else {
            currentWholeMessageBuffer!!.clear()
        }

        currentWholeMessageBuffer!!.limit(currentMessageLength)
        currentWholeMessageBuffer!!.put(buffer)
    }

    /**
     * Assumes there is data to be read in [buffer]. Copies data in the buffer
     * such that the remaining data starts at position 0.
     */
    private fun maximizeBufferRemaining() {
        if (buffer.hasRemaining() && buffer.position() != 0) {
            val slice = buffer.slice()
            buffer.position(0)
            buffer.limit(slice.limit())
            buffer.put(slice)
            buffer.flip()
        }
    }

    private fun createMessage(source: ByteBuffer, length: Int): T {
        assert(length <= source.remaining())

        val positionBefore = source.position()
        val limitBefore = source.limit()

        // so that parseFrom does not read more data than it should
        source.limit(positionBefore + length)

        try {
            return typeClass
                .getMethod("parseFrom", ByteBuffer::class.java)
                .invoke(null, source) as T
        }
        catch (ex: InvocationTargetException) {
            throw ex.cause!!
        }
        finally {
            // no matter what happens to the buffers pointers
            // set them to after the message
            source.limit(max(limitBefore, positionBefore + length))
            source.position(positionBefore + length)
        }
    }

    private val handler = object : CompletionHandler<Int, Nothing?> {
        override fun completed(nBytesRead: Int, attachment: Nothing?) {
            if (nBytesRead <= 0) {
                if (mode != MODE_LENGTH || digitSignificance > 1) {
                    // EOF within message || EOF within varint
                    onError.accept(InvalidProtocolBufferException("Encountered EOF within a variable uint32"))
                }
                else {
                    onChannelEnd.call()
                }

                return
            }

            buffer.flip()
            readAndContinue()
        }

        override fun failed(ex: Throwable?, attachment: Nothing?) {
            if (ex is ClosedChannelException) {
                // done
                onChannelEnd.call()
            } else {
                onError.accept(ex ?: IOException("Unknown channel read error"))
            }
        }
    }

    init {
        buffer.position(0)
        buffer.limit(0)

        readAndContinue()
    }
}