package com.github.prologdb.net

import com.github.prologdb.io.util.ByteArrayOutputStream
import com.github.prologdb.io.util.Pool
import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.InvalidProtocolBufferException
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.subjects.PublishSubject
import io.reactivex.subjects.SingleSubject
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.CompletionHandler
import kotlin.experimental.and
import kotlin.math.max

// buffer space for all purposes
val FourKBufferPool: Pool<ByteArrayOutputStream> = Pool(128, { ByteArrayOutputStream(4096) }, ByteArrayOutputStream::reset)

/**
 * Reads delimited protobuf objects from an [AsynchronousByteChannel]. The messages are published via [observable].
 */
class AsyncByteChannelDelimitedProtobufReader<T : GeneratedMessageV3>(
    private val typeClass: Class<T>,
    private val channel: AsynchronousByteChannel
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

    // for reading the object
    private val _observable = PublishSubject.create<T>()
    val observable: Observable<T>
    init {
        val publicObsv = _observable.replay()
        publicObsv.connect()
        observable = publicObsv
    }

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
                _observable.onError(IOException("Variable uint larger than 32 bits!"))
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
                    _observable.onNext(createMessage(buffer, currentMessageLength))
                }
                catch (ex: Throwable) {
                    _observable.onError(ex)
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
            _observable.onNext(createMessage(wholeMessageBuffer, currentMessageLength))

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
            FourKBufferPool.using { tmpBuffer ->
                tmpBuffer.bufferOfData.put(buffer)
                buffer.clear()
                tmpBuffer.bufferOfData.flip()
                buffer.put(tmpBuffer.bufferOfData)
                buffer.flip()
            }
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

fun GeneratedMessageV3.writeDelimitedTo(out: AsynchronousByteChannel): Single<Unit> {
    val bufferStream = FourKBufferPool.get()
    writeDelimitedTo(bufferStream)
    val singleSource = SingleSubject.create<Unit>()

    out.write(bufferStream.bufferOfData, null, object : CompletionHandler<Int, Nothing?> {
        private var nBytesToWrite = bufferStream.bufferOfData.remaining()

        override fun completed(result: Int, attachment: Nothing?) {
            nBytesToWrite -= result
            if (nBytesToWrite > 0) {
                // still data to write, try more
                out.write(bufferStream.bufferOfData, null, this)
            }
            else {
                // done
                FourKBufferPool.free(bufferStream)
                singleSource.onSuccess(Unit)
            }
        }

        override fun failed(ex: Throwable, attachment: Nothing?) {
            FourKBufferPool.free(bufferStream)
            singleSource.onError(ex)
        }
    })

    return singleSource
}

// intended for [AsynchronousByteChannel.readVarUInt32]
val OneByteBufferPool: Pool<ByteBuffer> = Pool(8, { ByteBuffer.allocate(1) }, { assert(it.capacity() == 1); it.clear() })

fun AsynchronousByteChannel.readVarUInt32(): Single<Long> {
    val singleSource = SingleSubject.create<Long>()

    object : CompletionHandler<Int, Nothing?> {
        private var carry: Long = 0
        private var digitSignificance: Long = 1L

        private var buffer: ByteBuffer = OneByteBufferPool.get()

        init {
            read()
        }

        override fun completed(result: Int, attachment: Nothing?) {
            if (result > 0) {
                buffer.flip()
                val byte = buffer.get()
                val value = if (byte >= 0) byte else byte and 0b01111111

                carry += value.toLong() * digitSignificance

                if (carry and 0xFFFFFFFFL != carry) {
                    // larger than 32 bits
                    singleSource.onError(IOException("Variable uint larger than 32 bits!"))
                    return
                }

                if (byte >= 0) {
                    // last
                    singleSource.onSuccess(carry)
                    return
                }
            }

            read()
        }

        override fun failed(exc: Throwable, attachment: Nothing?) {
            OneByteBufferPool.free(buffer)
            singleSource.onError(exc)
        }

        private fun read() {
            buffer.clear()
            read(buffer, null, this)
        }
    }

    return singleSource
}

fun <T : GeneratedMessageV3> AsynchronousByteChannel.readSingleDelimited(typeClass: Class<T>): Single<T> {
    if (typeClass == GeneratedMessageV3::class.java) {
        throw IllegalArgumentException("\$typeClass must be a subclass of ${GeneratedMessageV3::class.java.name}")
    }

    return readVarUInt32().flatMap { messageLengthLong ->
        val messageLength = messageLengthLong.toInt()

        val bufferObtainedFrom4KPool = messageLength <= 4096
        val streamFromPool: ByteArrayOutputStream? = if (bufferObtainedFrom4KPool) FourKBufferPool.get() else null
        val buffer = if (bufferObtainedFrom4KPool) streamFromPool!!.bufferOfData else ByteBuffer.allocateDirect(messageLength)
        buffer.clear()
        buffer.limit(messageLength)

        val singleSubject = SingleSubject.create<T>()

        object : CompletionHandler<Int, Nothing?> {

            init {
                read()
            }

            override fun completed(result: Int, attachment: Nothing?) {
                if (result < 0) {
                    singleSubject.onError(IOException("Unexpected EOF within message ${typeClass.simpleName}"))
                    return
                }

                if (buffer.remaining() == 0) {
                    // all bytes collected
                    buffer.flip()

                    try {
                        singleSubject.onSuccess(typeClass
                            .getMethod("parseFrom", ByteBuffer::class.java)
                            .invoke(null, buffer) as T)
                    }
                    catch (ex: Throwable) {
                        singleSubject.onError(ex)
                    }
                }
                else {
                    read()
                }
            }

            override fun failed(exc: Throwable, attachment: Nothing?) {
                singleSubject.onError(exc)
            }

            private fun read() {
                read(buffer, null, this)
            }
        }

        singleSubject.doFinally {
            if (bufferObtainedFrom4KPool) {
                FourKBufferPool.free(streamFromPool!!)
            }
        }

        return@flatMap singleSubject
    }
}