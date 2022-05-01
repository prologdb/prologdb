@file:JvmName("ProtobufAsync")
package com.github.prologdb.net.async

import com.github.prologdb.io.util.ByteArrayOutputStream
import com.github.prologdb.io.util.Pool
import com.google.protobuf.GeneratedMessageV3
import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.CompletionHandler
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.experimental.and

// buffer space for all purposes
val FourKBufferPool: Pool<ByteArrayOutputStream> = Pool(128, { ByteArrayOutputStream(4096) }, ByteArrayOutputStream::reset)

/**
 * @receiver The message to be sent
 * @param reportTo When done, completes this future with the message written; on error, completes exceptionally
 */
fun <T : GeneratedMessageV3> T.writeDelimitedTo(out: AsynchronousByteChannel, reportTo: CompletableFuture<Unit>? = null) {
    val bufferStream = FourKBufferPool.get()
    writeDelimitedTo(bufferStream)

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
                reportTo?.complete(Unit)
            }
        }

        override fun failed(ex: Throwable, attachment: Nothing?) {
            FourKBufferPool.free(bufferStream)
            reportTo?.completeExceptionally(ex)
        }
    })
}

/**
 * A pool of buffers of size 1. intended for [AsynchronousByteChannel.readVarUInt32]
 */
private val OneByteBufferPool: Pool<ByteBuffer> = Pool(8, { ByteBuffer.allocate(1) }, { assert(it.capacity() == 1); it.clear() })

fun AsynchronousByteChannel.readVarUInt32(): CompletionStage<Long> {
    val future = CompletableFuture<Long>()

    object : CompletionHandler<Int, Nothing?> {
        private var carry: Long = 0
        private var digitSignificance: Long = 1L

        private var buffer: ByteBuffer = OneByteBufferPool.get()

        init {
            read()
        }

        override fun completed(result: Int, attachment: Nothing?) {
            if (result < 0) {
                future.completeExceptionally(IOException("Unexpected EOF in variable uint"))
                return
            }

            if (result > 0) {
                buffer.flip()
                val byte = buffer.get()
                val value = if (byte >= 0) byte else byte and 0b01111111

                carry += value.toLong() * digitSignificance
                digitSignificance = digitSignificance shl 7

                if (carry and 0xFFFFFFFFL != carry) {
                    // larger than 32 bits
                    future.completeExceptionally(IOException("Variable uint larger than 32 bits!"))
                    return
                }

                if (byte >= 0) {
                    // last
                    future.complete(carry)
                    return
                }
            }

            read()
        }

        override fun failed(exc: Throwable, attachment: Nothing?) {
            OneByteBufferPool.free(buffer)
            future.completeExceptionally(exc)
        }

        private fun read() {
            buffer.clear()
            read(buffer, null, this)
        }
    }

    return future
}

fun <T : GeneratedMessageV3> AsynchronousByteChannel.readSingleDelimited(typeClass: Class<T>): CompletionStage<T> {
    if (typeClass == GeneratedMessageV3::class.java) {
        throw IllegalArgumentException("\$typeClass must be a subclass of ${GeneratedMessageV3::class.java.name}")
    }

    return readVarUInt32().thenCompose { messageLengthLong ->
        val messageLength = messageLengthLong.toInt()

        val bufferObtainedFrom4KPool = messageLength <= 4096
        val streamFromPool: ByteArrayOutputStream? = if (bufferObtainedFrom4KPool) FourKBufferPool.get() else null
        val buffer = if (bufferObtainedFrom4KPool) streamFromPool!!.bufferOfData else ByteBuffer.allocateDirect(messageLength)
        buffer.clear()
        buffer.limit(messageLength)

        val messageFuture = CompletableFuture<T>()
        messageFuture.whenComplete { _, _ ->
            if (bufferObtainedFrom4KPool) {
                FourKBufferPool.free(streamFromPool!!)
            }
        }

        object : CompletionHandler<Int, Nothing?> {

            init {
                read()
            }

            override fun completed(result: Int, attachment: Nothing?) {
                if (result < 0) {
                    messageFuture.completeExceptionally(IOException("Unexpected EOF within message ${typeClass.simpleName}"))
                    return
                }

                if (buffer.remaining() == 0) {
                    // all bytes collected
                    buffer.flip()

                    try {
                        @Suppress("UNCHECKED_CAST")
                        messageFuture.complete(typeClass
                            .getMethod("parseFrom", ByteBuffer::class.java)
                            .invoke(null, buffer) as T)
                    }
                    catch (ex: InvocationTargetException) {
                        messageFuture.completeExceptionally(ex.cause!!)
                    }
                    catch (ex: Throwable) {
                        messageFuture.completeExceptionally(ex)
                    }
                }
                else {
                    read()
                }
            }

            override fun failed(exc: Throwable, attachment: Nothing?) {
                messageFuture.completeExceptionally(exc)
            }

            private fun read() {
                read(buffer, null, this)
            }
        }

        return@thenCompose messageFuture
    }.toCompletableFuture()
}