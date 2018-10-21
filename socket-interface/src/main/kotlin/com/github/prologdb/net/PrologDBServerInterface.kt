package com.github.prologdb.net

import com.github.prologdb.async.LazySequence
import com.github.prologdb.net.session.*
import com.github.prologdb.net.session.handle.SessionHandle
import com.github.prologdb.runtime.unification.Unification
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

    private inner class Worker(private val queryHandler: QueryHandler) : Runnable {

        private var closed = false
        private lateinit var thread: Thread

        override fun run() {
            thread = Thread.currentThread()

            while (!closed) {
                val closedContexts = mutableListOf<Int>() // local to the second loop but hoisted for efficiency

                for ((sessionHandle, contexts) in queryContexts) {
                    closedContexts.clear()
                    for ((queryId, context) in contexts) {
                        context.ifAvailable {
                            it.step()
                            val closed = it.consumeSolutions(
                                solutionHandler = { solution ->
                                    sessionHandle.queueMessage(QuerySolutionMessage(queryId, solution))
                                },
                                errorHandler = { error ->
                                    sessionHandle.queueMessage(error.queryRelatedErrorToMessage(queryId))
                                },
                                closeNotifier = { reason ->
                                    sessionHandle.queueMessage(QueryClosedMessage(queryId, reason))
                                }
                            )
                            if (closed) {
                                closedContexts.add(queryId)
                            }
                        }

                        // do an open task
                        tasks.poll()?.let(::doTask)
                    }

                    closedContexts.forEach { contexts.remove(it) }
                }
            }
        }

        private fun doTask(task: Task) {
            when (task.message) {
                is InitializeQueryCommand -> initializeQuery(task.message, task.handle)
                is ConsumeQuerySolutionsCommand -> registerConsumptionRequest(task.message, task.handle)
                is GeneralError -> handleClientError(task.message, task.handle)
                else -> throw IllegalArgumentException("Tasks with message of type ${task.message.javaClass.name} not supported")
            }
        }

        private fun registerConsumptionRequest(command: ConsumeQuerySolutionsCommand, handle: SessionHandle) {
            val contexts = queryContexts.computeIfAbsent(handle) { ConcurrentHashMap() }
            val context = contexts[command.queryId] ?: throw QueryRelatedException(
                QueryRelatedError(
                    command.queryId,
                    QueryRelatedError.Kind.QUERY_ID_NOT_IN_USE,
                    "Cannot consume solutions of query ${command.queryId.unsingedIntHexString()}: not initialized"
                )
            )

            val (wasAvailable, _) = context.ifAvailable {
                it.registerConsumptionRequest(command)
            }
            if (!wasAvailable) {
                // requeue for a retry
                tasks.put(Task(handle, command))
                // TODO: log&track requeues and determine whether it is a performance-issue
            }
        }

        private fun initializeQuery(command: InitializeQueryCommand, handle: SessionHandle) {
            fun refuseForDuplicateID() {
                throw QueryRelatedException(
                    QueryRelatedError(
                        command.desiredQueryId,
                        QueryRelatedError.Kind.QUERY_ID_ALREADY_IN_USE,
                        "QueryID ${command.desiredQueryId.unsingedIntHexString()} is currently in use",
                        mapOf("queryId" to command.desiredQueryId.unsingedIntHexString())
                    )
                )
            }

            val contexts = queryContexts.computeIfAbsent(handle) { ConcurrentHashMap() }
            if (command.desiredQueryId in contexts.keys) {
                refuseForDuplicateID()
            }

            // initialize the context
            val queryContext = QueryContext(
                command.desiredQueryId,
                command.startUsing(queryHandler)
            )

            // catch the race condition
            val queryIdPresent = contexts.putIfAbsent(command.desiredQueryId, queryContext) != null
            if (queryIdPresent) {
                refuseForDuplicateID()
            }
        }

        private fun handleClientError(error: GeneralError, handle: SessionHandle) {
            // TODO: log
            val contexts = queryContexts.remove(handle)
            if (contexts != null) {
                contexts.values.forEach { context ->
                    context.doWith { it.close() }
                }
            }
            handle.closeSession()
        }

        private fun InitializeQueryCommand.startUsing(handler: QueryHandler): LazySequence<Unification> {
            return when (kind) {
                InitializeQueryCommand.Kind.QUERY     -> handler.startQuery(instruction, totalLimit)
                InitializeQueryCommand.Kind.DIRECTIVE -> handler.startDirective(instruction, totalLimit)
            }
        }

        fun close() {
            closed = true
            if (this::thread.isInitialized) {
                if (this.thread != Thread.currentThread()) {
                    this.thread.interrupt()
                    this.thread.join()
                }
            }
        }
    }
}

