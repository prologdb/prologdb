package com.github.prologdb.net.async

import com.google.protobuf.GeneratedMessageV3
import java.nio.channels.AsynchronousByteChannel
import java.util.concurrent.Future

/**
 * A wrapper for [AsynchronousByteChannel] that queues outgoing protobuf3 messages in order to prevent
 * [WritePendingException]s.
 */
class AsyncChannelProtobufOutgoingQueue(wrapped: AsynchronousByteChannel) {
    private val pipe = AsyncPipe<GeneratedMessageV3, Unit> { message, onDone ->
        message.writeDelimitedTo(wrapped, onDone)
    }

    fun queue(message: GeneratedMessageV3): Future<Unit> {
        return pipe.queue(message)
    }

    /**
     * Blocks until the writing of all messages to the underlying stream
     * has begun, then closes this object (but not the underlying [AsynchronousByteChannel]!).
     */
    fun close() = pipe.close()

    val closed = pipe.closed
}