package com.github.prologdb.net

import com.google.protobuf.GeneratedMessageV3
import io.reactivex.Single
import io.reactivex.rxkotlin.subscribeBy
import java.nio.channels.AsynchronousByteChannel
import java.util.concurrent.LinkedBlockingQueue

/**
 * A wrapper for [AsynchronousByteChannel] that queues outgoing protobuf3 messages in order to prevent
 * [WritePendingException]s.
 */
class AsyncChannelProtobufOutgoingQueue(private val wrapped: AsynchronousByteChannel) {
    private val outQueue = LinkedBlockingQueue<GeneratedMessageV3>()
    @Volatile
    private var currentSending: Single<Unit>? = null
    private val currentSendingMutex = Any()

    private val closingMutex = Any()
    private var closed = false

    fun queue(message: GeneratedMessageV3) {
        synchronized(currentSendingMutex) {
            if (currentSending == null) {
                // currently no sending going on, go directly
                send(message)
                return
            }
        }

        // currently something is sending, queue for later delivery
        outQueue.put(message)
    }

    /**
     * Sends the message and sets currentSending. Assumes exclusive access to currentSending
     */
    private fun send(message: GeneratedMessageV3) {
        val currentSendingBefore = synchronized(currentSendingMutex) {
            currentSending = message.writeDelimitedTo(wrapped)
            currentSending
        }

        currentSending!!.subscribeBy {
            synchronized(currentSendingMutex) {
                if (currentSending === currentSendingBefore) {
                    currentSending = null
                    outQueue.poll()?.let { send(it) }
                }
            }
        }
    }

    /**
     * Blocks until all queued messages have been sent, then closes this object (but not the
     * underlying [AsynchronousByteChannel]!).
     */
    fun close() {
        if (closed) return
        synchronized(closingMutex) {
            if (closed) return
            closed = true
        }

        while (outQueue.isNotEmpty()) {
            try {
                Thread.sleep(100)
            }
            catch (ex: InterruptedException) {}
        }
    }
}