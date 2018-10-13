package com.github.prologdb.net

import com.github.prologdb.net.session.ProtocolMessage
import com.github.prologdb.net.session.QueryHandler
import com.github.prologdb.net.session.SessionInitializer
import com.github.prologdb.net.session.handle.SessionHandle
import java.io.IOException
import java.net.ServerSocket
import java.net.Socket
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.function.Supplier

/**
 * Opens a new interface on the given [ServerSocket]
 */
class PrologDBServerInterface(
    private val serverSocket: ServerSocket,

    /**
     * For each worker thread spawned, one handler is obtained from this supplier. If this supplier returns the
     * same instance for multiple invocations, the instance MUST be thread-safe.
     */
    private val queryHandlerSupplier: Supplier<QueryHandler>,

    /**
     * Used to do the handshakes. This is where support for multiple protocols can be added.
     */
    private val sessionInitializer: SessionInitializer,

    /**
     * The number of worker threads to spawn. The worker threads will spawn the actual
     * queries through [QueryHandler.startQuery] and [QueryHandler.startDirective] as well
     * as calculate the solutions using [LazySequence.tryAdvance].
     */
    private val nWorkerThreads: Int = 2
) {
    /**
     * A reference to the thread that accepts new connections and launches the [ConnectionIOHandler] threads.
     */
    private lateinit var accepterThread: Thread

    /**
     * Whether this interface is actively accepting connections
     */
    val isActive: Boolean
        get() = ::accepterThread.isInitialized && accepterThread.isAlive

    /**
     * Set to true once
     */
    private var isClosed = false

    private val workers: MutableSet<Pair<Worker, Thread>> = Collections.newSetFromMap(ConcurrentHashMap())

    private val startupMutex = Any()
    fun startAcceptingClients(withThisThread: Boolean = false) {
        synchronized(startupMutex) {
            if (isActive) {
                throw IllegalStateException("Already listening for connections.")
            }

            for (i in 1..nWorkerThreads) {
                val worker = Worker(tasks, queryHandlerSupplier.get())
                val thread = Thread(worker, "prologdb-${serverSocket.localSocketAddress}-worker-$i")
                workers.add(Pair(worker, thread))
            }

            if (withThisThread) {
                accepterThread = Thread.currentThread()
                accepterLoop()
            } else {
                accepterThread = Thread(this::accepterLoop, "prologdb-${serverSocket.localSocketAddress}-connection-accept")
                accepterThread.start()
            }
        }
    }

    /**
     * [ServerSocket.accept]s new connections and puts them onto [newConnections].
     * Runs until the server socket is closed.
     */
    private fun accepterLoop() {
        while (true) {
            val socket = serverSocket.accept()
            Thread(ConnectionIOHandler(socket), "prologdb-connio-${socket.localAddress}}").start()
        }
    }

    /**
     * The [ConnectionIOHandler] place incoming messages into this queue for the worker threads to pick them up.
     */
    private val tasks = LinkedBlockingQueue<Task>()

    private val closingMutex = Any()
    fun close() {
        synchronized(closingMutex) {
            if (isActive) {
                if (!serverSocket.isClosed) serverSocket.close()
            }

            workers.forEach { it.first.close(); }
            workers.removeAll(workers.filter { !it.second.isAlive })
        }
    }

    /**
     * Is responsible for the IO of one TCP connection. This is because the JVM does not
     * support socket I/O with events or coroutines.
     */
    private inner class ConnectionIOHandler(private val socket: Socket) : Runnable {
        override fun run() {
            val handle = sessionInitializer.init(socket)
            try {
                tasks.put(
                    Task(
                        handle,
                        handle.popNextIncomingMessage()
                    )
                )
            }
            catch (ex: IOException) {
                System.err.println(ex)
                // TODO: handle gracefully, e.g. socket closes
            }
        }
    }

    private data class Task(
        val handle: SessionHandle,
        val message: ProtocolMessage
    )

    private class Worker(
        private val taskSource: LinkedBlockingQueue<Task>,
        private val queryHandler: QueryHandler
    ) : Runnable {

        private var closed = false
        private lateinit var thread: Thread

        override fun run() {
            thread = Thread.currentThread()

            while (!closed) {
                try {
                    val task = taskSource.take()
                }
                catch (ex: InterruptedException) {
                    continue
                }

                TODO("do the task")
            }
        }

        fun close() {
            closed = true
            if (this::thread.isInitialized) {
                this.thread.interrupt()
                this.thread.join()
            }
        }
    }
}