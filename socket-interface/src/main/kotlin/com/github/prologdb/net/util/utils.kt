package com.github.prologdb.net.util

import com.github.prologdb.io.util.ByteArrayOutputStream
import com.github.prologdb.io.util.Pool
import com.google.protobuf.GeneratedMessageV3
import io.reactivex.Single
import io.reactivex.subjects.SingleSubject
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.CompletionHandler

// buffer space for all purposes
val FourKBufferPool: Pool<ByteArrayOutputStream> = Pool(128, { ByteArrayOutputStream(4096) }, ByteArrayOutputStream::reset)

fun ByteBuffer.putVarUInt32(value: Int) {
    if (value < 0) throw IllegalArgumentException()

    var carry = value
    do {
        var byteValue = carry and 0b01111111
        carry = carry ushr 7
        if (carry != 0) {
            // not the last
            byteValue = -byteValue
        }
        put(byteValue.toByte())
    } while (carry > 0)
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