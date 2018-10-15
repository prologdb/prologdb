package com.github.prologdb.net

import com.github.prologdb.net.session.*
import com.github.prologdb.net.session.handle.SessionHandle
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.unification.Unification
import java.io.IOException
import java.net.ServerSocket
import java.net.Socket
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Supplier
import kotlin.math.min

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
                val worker = Worker(queryHandlerSupplier.get())
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
                // TODO: handle gracefully, probably involves heartbeating and/or an explicit close message
            }
        }
    }

    private data class Task(
        val handle: SessionHandle,
        val message: ProtocolMessage
    )

    private val queryContexts: MutableMap<SessionHandle, MutableMap<Int, QueryContext>> = ConcurrentHashMap()

    private inner class Worker(
        private val queryHandler: QueryHandler
    ) : Runnable {

        private var closed = false
        private lateinit var thread: Thread

        override fun run() {
            thread = Thread.currentThread()

            while (!closed) {
                /*
                 * Admittedly, this code is cryptic. So here is how it works:
                 * as long as there are no tasks available (using tasks.poll),
                 * it does precalculations (one at a time before checking again).
                 * If all precalculations are done it will wait for and then execute
                 * the next task (ending the loop and waiting using tasks.take).
                 */
                do {
                    // look for new tasks
                    val hasPrecalculationsTodo = doOnePrecalculation()
                    val hasTask = tasks.peek() != null
                }
                while (!hasTask && hasPrecalculationsTodo)

                // precalculations done, do a task (possibly waiting for it)
                doTask(tasks.take())
            }
        }

        private fun doTask(task: Task) {
            when (task.message) {
                is InitializeQueryCommand -> initializeQuery(task.message, task.handle)
                is ConsumeQuerySolutionsCommand -> consumeQuerySolutions(task.message, task.handle)
                is GeneralError -> handleClientError(task.message, task.handle)
                else -> throw IllegalArgumentException("Tasks with message of type ${task.message.javaClass.name} not supported")
            }
        }

        /**
         * @return Whether there are more precalculations outstanding that can
         *         be started right away. Even when false is returned, some
         *         might be outstanding. But in order not to wait for a shared lock,
         *         it will be assumed that locked context do not have any outstanding
         *         precalculations.
         */
        private fun doOnePrecalculation(): Boolean {
            for (contexts in queryContexts.values) {
                for (context in contexts.values) {
                    if (context.lock.tryLock()) {
                        try {
                            val hasMore = context.doOnePrecalculation()
                            if (hasMore) return true
                        }
                        finally {
                            context.lock.unlock()
                        }
                    }
                }
            }

            return false
            // there might actually be more, but these are currently
            // locked by other threads. returning false makes the
            // worker continue waiting for user-specified tasks. This
            // behaviour thus puts user tasks at a higher priority than
            // precalculations, so, while not technically correct, this
            // return is desireable in its current form.
        }

        private fun initializeQuery(command: InitializeQueryCommand, handle: SessionHandle) {
            TODO()
        }

        private fun consumeQuerySolutions(command: ConsumeQuerySolutionsCommand, handle: SessionHandle) {
            TODO()
        }

        private fun handleClientError(error: GeneralError, handle: SessionHandle) {
            TODO()
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

/**
 * Context of a query. Only one thread can work on one
 * query context at a time. The worker threads synchronize
 * using the [lock].
 */
private class QueryContext(
    private val solutions: LazySequence<Unification>,
    private val precalculation_limit: Long,
    private val total_limit: Long
) {
    val lock: Lock = ReentrantLock()

    private val precalculations: Queue<Unification> = ArrayDeque(min(precalculation_limit, 1024L).toInt())

    private val hasPrecalculationsOutstanding: Boolean
        get() = precalculations.size.toLong() < precalculation_limit

    /**
     * If needed, does one precalculation.
     * @return whether there are more outstanding.
     */
    fun doOnePrecalculation(): Boolean {
        if (!hasPrecalculationsOutstanding) return false

        val solution = solutions.tryAdvance()
        // this wont block or trip because ArrayDeque
        assert(precalculations.offer(solution), { "Precalculations queue out of capacity. Somethings wrong here!" })

        return hasPrecalculationsOutstanding
    }
}