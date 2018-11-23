package com.github.prologdb.net

import com.github.prologdb.io.util.ByteArrayOutputStream
import com.github.prologdb.net.async.AsyncByteChannelDelimitedProtobufReader
import com.github.prologdb.net.v1.messages.Query
import com.github.prologdb.net.v1.messages.QueryInitialization
import com.github.prologdb.net.v1.messages.ToServer
import com.google.protobuf.ByteString
import io.kotlintest.matchers.beGreaterThan
import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.specs.FreeSpec
import io.mockk.every
import io.mockk.mockkClass
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.CompletionHandler
import java.nio.charset.Charset
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer

class AsyncByteChannelDelimitedProtobufReaderTest : FreeSpec({
    val channel = mockkClass(AsynchronousByteChannel::class)

    val message = ToServer.newBuilder()
        .setInitQuery(QueryInitialization.newBuilder()
            .setQueryId(1)
            .setKind(QueryInitialization.Kind.QUERY)
            .setInstruction(Query.newBuilder()
                .setType(Query.Type.STRING)
                .setData(ByteString.copyFrom(
                    "foo(baz(Z), 2), bar(3, { a: 3 }, \"this is intentionally long as to test" +
                    "the varint decoding. It really comes to play with lengths over 128 bytes." +
                    "So this should pose a real test for the varint decoding.\")",
                    Charset.defaultCharset()
                ))
                .build()
            )
            .build()
        )
        .build()

    val outBuffer = ByteArrayOutputStream()
    message.writeDelimitedTo(outBuffer)

    outBuffer.bufferOfData.remaining() should beGreaterThan(128) // if not, the varint decoding is not tested properly

    "message as a whole" {
        var nInvocation = 0
        every { channel.read(any(), any() as Any?, any()) } answers { call ->
            nInvocation++
            val buffer = call.invocation.args[0] as ByteBuffer
            if (nInvocation == 1) {
                val nBytes = outBuffer.bufferOfData.remaining()
                buffer.put(outBuffer.bufferOfData)

                (call.invocation.args[2] as CompletionHandler<Int, Any?>).completed(nBytes, call.invocation.args[1])
            }
            else {
                (call.invocation.args[2] as CompletionHandler<Int, Any?>).completed(-1, call.invocation.args[1])
            }
        }

        // ACT
        val messagesRead = ArrayList<ToServer>()
        var completeCalled = CompletableFuture<Unit>()

        AsyncByteChannelDelimitedProtobufReader(ToServer::class.java, channel,
            Consumer { messagesRead.add(it) },
            Consumer { throw it },
            Callable<Unit> { completeCalled.complete(Unit) }
        )
        completeCalled.get()

        // ASSERT
        messagesRead.size shouldBe 1
        val readMessage = messagesRead[0]

        readMessage shouldBe message
    }

    "message split" {
        // SETUP
        var nInvocation = 0
        every { channel.read(any(), any() as Any?, any()) } answers { call ->
            nInvocation++
            val buffer = call.invocation.args[0] as ByteBuffer
            if (nInvocation == 1) {
                for (i in 0..6) {
                    buffer.put(outBuffer.bufferOfData.get())
                }

                (call.invocation.args[2] as CompletionHandler<Int, Any?>).completed(7, call.invocation.args[1])
            }
            else if (nInvocation == 2) {
                val nBytes = outBuffer.bufferOfData.remaining()
                buffer.put(outBuffer.bufferOfData)
                (call.invocation.args[2] as CompletionHandler<Int, Any?>).completed(nBytes, call.invocation.args[1])
            }
            else {
                (call.invocation.args[2] as CompletionHandler<Int, Any?>).completed(-1, call.invocation.args[1])
            }
        }

        // ACT
        val messagesRead = ArrayList<ToServer>()
        var completeCalled = CompletableFuture<Unit>()

        AsyncByteChannelDelimitedProtobufReader(ToServer::class.java, channel,
            Consumer { messagesRead.add(it) },
            Consumer { throw it },
            Callable<Unit> { completeCalled.complete(Unit) }
        )
        completeCalled.get()

        // ASSERT
        messagesRead.size shouldBe 1
        val readMessage = messagesRead[0]

        readMessage shouldBe message
    }
}) {
    override fun isInstancePerTest(): Boolean = true
}