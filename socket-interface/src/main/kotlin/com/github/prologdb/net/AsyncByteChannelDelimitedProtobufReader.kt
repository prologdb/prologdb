package com.github.prologdb.net

import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.InvalidProtocolBufferException
import io.reactivex.Observable
import io.reactivex.subjects.PublishSubject
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.CompletionHandler
import kotlin.experimental.and

/**
 * Reads delimited protobuf objects from an [AsynchronousByteChannel].
 */
class AsyncByteChannelDelimitedProtobufReader<T : GeneratedMessageV3>(
    private val typeClass: Class<T>,
    private val channel: AsynchronousByteChannel,
    /** The total max number of messages to read, -1 for infinite */
    totalLimit: Int = -1
) {

    init {
        if (typeClass == GeneratedMessageV3::class.java) {
            throw IllegalArgumentException("\$typeClass must be a subclass of ${GeneratedMessageV3::class.java.name}")
        }

        if (totalLimit == 0) {
            throw IllegalArgumentException("The total limit must not be 0.")
        }
    }

    private val buffer = ByteBuffer.allocateDirect(4096)

    private val MODE_LENGTH = true
    private val MODE_MESSAGE = false
    private var mode = MODE_LENGTH

    // for reading the length
    private var carry: Long = 0
    private var digitSignificance: Long = 1

    // for reading the object
    private val _observable = PublishSubject.create<T>()
    val observable: Observable<T>
    init {
        val publicObsv = _observable.replay()
        publicObsv.connect()
        observable = publicObsv
    }

    /**
     * The number of messages that can still be read from the channel. If negative,
     * there is not limit.
     */
    private var remaining = totalLimit

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
                mode = MODE_MESSAGE
                readAndContinueMessage()
                return
            }

            if (byte >= 0) {
                // last
                mode = MODE_MESSAGE
                readAndContinueMessage()
                return
            }

            digitSignificance *= 128L
        }

        // not done yet, otherwise this code wouldn't execute
        if (buffer.position() == buffer.limit()) {
            buffer.position(0)
            buffer.mark()
            buffer.limit(buffer.capacity())
        }

        channel.read(buffer, null, handler)
    }

    private fun readAndContinueMessage() {
        val messageLength = carry.toInt()
        val amountAlreadyPresent = buffer.remaining()
        if (amountAlreadyPresent >= messageLength) {
            try {
                _observable.onNext(createMessage(buffer, messageLength))
            }
            catch (ex: Throwable) {
                _observable.onError(ex)
            }

            if (remaining > 0) remaining--
            if (remaining != 0) {
                // if remaining was positive, it is still positive -> continue, more to go
                // if remaining was negative to begin with, its != 0

                mode = MODE_LENGTH
                readAndContinueLength()
            } else {
                _observable.onComplete()
            }
        }
    }

    private fun createMessage(source: ByteBuffer, length: Int): T {
        assert(length <= source.remaining())

        val positionBefore = source.position()
        val limitBefore = source.limit()

        // so that parseFrom does not read more data than it should
        source.limit(source.position() + length)

        try {
            return typeClass
                .getMethod("parseFrom", ByteBuffer::class.java)
                .invoke(null, source) as T
        }
        finally {
            // no matter what happens to the buffers pointers
            // set them to after the message
            buffer.limit(limitBefore)
            buffer.position(positionBefore + length)
        }
    }

    private val handler = object : CompletionHandler<Int, Nothing?> {
        override fun completed(nBytesRead: Int, attachment: Nothing?) {
            if (nBytesRead <= 0) {
                if (mode != MODE_LENGTH || digitSignificance > 1) {
                    // EOF within message || EOF within varint
                    _observable.onError(InvalidProtocolBufferException("Encountered EOF within a variable uint32"))
                }
                else {
                    _observable.onComplete()
                }

                return
            }

            buffer.flip()
            readAndContinue()
        }

        override fun failed(ex: Throwable?, attachment: Nothing?) {
            _observable.onError(ex ?: IOException("Unknown channel read error"))
        }
    }

    init {
        buffer.position(0)
        buffer.limit(0)

        readAndContinue()
    }
}