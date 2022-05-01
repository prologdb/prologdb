package com.github.prologdb.net.async

import com.google.protobuf.CodedOutputStream
import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.BehaviorSpec
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.CompletionHandler
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import kotlin.random.Random

class AsyncProtobufTest : BehaviorSpec ({
    Given("a varuint32 below 128") {
        val buffer = ByteBuffer.allocate(100)
        val int = Random.nextInt(0, 127)
        CodedOutputStream.newInstance(buffer).apply {
            writeUInt32NoTag(int)
            flush()
        }
        buffer.flip()
        val channel = asynchronousByteChannelOf(buffer)
        When("read") {
            Then("it reads correctly") {
                channel.readVarUInt32().toCompletableFuture().get() shouldBe int.toLong()
            }
        }
    }

    Given("a varuint32 below between 128 and 255") {
        val buffer = ByteBuffer.allocate(100)
        val int = /*Random.nextInt(128, 255)*/ 300
        CodedOutputStream.newInstance(buffer).apply {
            writeUInt32NoTag(int)
            flush()
        }
        buffer.flip()
        val channel = asynchronousByteChannelOf(buffer)
        When("read") {
            Then("it reads correctly") {
                channel.readVarUInt32().toCompletableFuture().get() shouldBe int.toLong()
            }
        }
    }

    Given("a varuint32 above 65536") {
        val buffer = ByteBuffer.allocate(100)
        val int = Random.nextInt(65536, Int.MAX_VALUE)
        CodedOutputStream.newInstance(buffer).apply {
            writeUInt32NoTag(int)
            flush()
        }
        buffer.flip()
        val channel = asynchronousByteChannelOf(buffer)
        When("read") {
            Then("it reads correctly") {
                channel.readVarUInt32().toCompletableFuture().get() shouldBe int.toLong()
            }
        }
    }
})

private fun asynchronousByteChannelOf(data: ByteBuffer) = object : AsynchronousByteChannel {
    @Volatile
    private var closed = false
    override fun close() {
        closed = true
    }

    override fun isOpen(): Boolean {
        return closed
    }

    override fun <A : Any?> read(dst: ByteBuffer, attachment: A, handler: CompletionHandler<Int, in A>) {
        handler.completed(read(dst).get(), attachment)
    }

    override fun read(dst: ByteBuffer): Future<Int> {
        if (!data.hasRemaining()) {
            return CompletableFuture.completedFuture(-1)
        }


        if (data.remaining() <= dst.remaining()) {
            val n = data.remaining()
            dst.put(data)
            return CompletableFuture.completedFuture(n)
        }

        val n = dst.remaining()
        dst.put(dst.position(), data, data.position(), n)
        dst.position(dst.position() + n)
        data.position(data.position() + n)
        return CompletableFuture.completedFuture(n)
    }

    override fun <A : Any?> write(src: ByteBuffer, attachment: A, handler: CompletionHandler<Int, in A>) {
        handler.completed( write(src).get(), attachment)
    }

    override fun write(src: ByteBuffer): Future<Int> {
        val n = src.remaining()
        src.position(src.position() + n)
        return CompletableFuture.completedFuture(n)
    }
}