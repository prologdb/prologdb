package com.github.prologdb.net

import com.github.prologdb.net.session.*
import com.github.prologdb.net.session.handle.SessionHandle
import com.github.prologdb.runtime.PrologRuntimeException
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
import kotlin.concurrent.withLock
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
                    val (wasAvailabe, hasMore) = context.ifAavailable { it.doOnePrecalculation() }

                    if (wasAvailabe && hasMore!!) {
                        return true
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
                command.startUsing(queryHandler),
                command.initialPrecalculationLimit
            )

            // catch the race condition
            val queryIdPresent = contexts.putIfAbsent(command.desiredQueryId, queryContext) != null
            if (queryIdPresent) {
                refuseForDuplicateID()
            }
        }

        private fun consumeQuerySolutions(command: ConsumeQuerySolutionsCommand, handle: SessionHandle) {
            val contexts = queryContexts.computeIfAbsent(handle) { ConcurrentHashMap() }
            val context = contexts[command.queryId] ?: throw QueryRelatedException(
                QueryRelatedError(
                    command.queryId,
                    QueryRelatedError.Kind.QUERY_ID_NOT_IN_USE,
                    "Cannot consume solutions of query ${command.queryId.unsingedIntHexString()}: not initialized"
                )
            )

            fun handleResults(solutions: Collection<Unification>) {
                when (command.handling) {
                    ConsumeQuerySolutionsCommand.SolutionHandling.DISCARD -> {}
                    ConsumeQuerySolutionsCommand.SolutionHandling.RETURN -> {
                        solutions.forEach {
                            handle.sendMessage(QuerySolutionMessage(command.queryId, it))
                        }
                    }
                }
            }

            // TODO: handle large amounts in smaller batches to distribute the load
            context.doWith {
                var closedForDepletion = false
                if (command.amount != 0) {
                    val precalculatedSolutions = ArrayList<Unification>(command.amount ?: 10)
                    it.drainPrecalculationsTo(precalculatedSolutions, command.amount?: Integer.MAX_VALUE)

                    handleResults(precalculatedSolutions)

                    if (command.amount == null) {
                        val batchsize = 100
                        do {
                            val solutions = ArrayList<Unification>(batchsize)
                            val nCalculated = it.calculateSolutionsInto(solutions, batchsize)
                            handleResults(solutions)
                        }
                        while (nCalculated == batchsize)
                        closedForDepletion = true
                        it.close()
                        handle.sendMessage(QueryClosedMessage(command.queryId, QueryClosedMessage.CloseReason.SOLUTIONS_DEPLETED))
                    }
                    else
                    {
                        val nToCalculate = command.amount - precalculatedSolutions.size
                        val solutions = ArrayList<Unification>(command.amount)
                        val nCalculated = it.calculateSolutionsInto(solutions, nToCalculate)
                        handleResults(solutions)

                        if (nCalculated < nToCalculate) {
                            closedForDepletion = true
                            handle.sendMessage(QueryClosedMessage(command.queryId, QueryClosedMessage.CloseReason.SOLUTIONS_DEPLETED))
                        }
                    }
                }

                if (command.precalculateLimitUpdate != null) {
                    it.precalculationLimit = command.precalculateLimitUpdate
                }

                if (command.closeAfterwards && !closedForDepletion) {
                    it.close()
                    if (command.notifyAboutClose) {
                        handle.sendMessage(QueryClosedMessage(command.queryId, QueryClosedMessage.CloseReason.ABORTED_ON_USER_REQUEST))
                    }
                }
            }
        }

        private fun handleClientError(error: GeneralError, handle: SessionHandle) {
            // TODO: log
            val contexts = queryContexts.remove(handle)
            if (contexts != null) {
                contexts.values.forEach {
                    it.doWith { it.close() }
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

/**
 * Context of a query. Only one thread can work on one
 * query context at a time. The worker threads synchronize
 * using the [lock].
 *
 * Query contexts are **NOT THREAD-SAFE!**. Use [ifAavailable]
 * for interaction.
 */
private class QueryContext(
    private val queryId: Int,
    private val solutions: LazySequence<Unification>,
    private var precalculationLimit: Long
) {
    private val lock: Lock = ReentrantLock()

    private val precalculations: Queue<Unification> = ArrayDeque(min(precalculationLimit, 1024L).toInt())

    private val hasPrecalculationsOutstanding: Boolean
        get() = precalculations.size.toLong() < precalculationLimit

    private val actionInterface = ActionInterface()

    /**
     * If currently no other thread is working on this context,
     * locks the context and executes the action. The return
     * value and exceptions are forwarded from the action. The lock
     * is released before this method returns in any case.
     *
     * @return first: whether the action was executed, second: the
     * forwarded return value
     */
    fun <T> ifAavailable(action: (ActionInterface) -> T): Pair<Boolean, T?> {
        if (lock.tryLock()) {
            try {
                return Pair(true, action(actionInterface))
            }
            finally {
                lock.unlock()
            }
        } else {
            return Pair(false, null)
        }
    }

    /**
     * Locks the context, possibly waiting interruptibly for the lock
     * to become available. Once acquired runs the given action. Return
     * value and exceptions are forwarded. The lock is released in any
     * case.
     */
    fun <T> doWith(action: (ActionInterface) -> T): T = lock.withLock { action(actionInterface) }

    inner class ActionInterface internal constructor() {
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

        /**
         * [MutableCollection.add]s up to `limit` precalculated solutions to `target`.
         *
         * @return the actual number of solutions added.
         */
        fun drainPrecalculationsTo(target: MutableCollection<in Unification>, limit: Int = Integer.MAX_VALUE): Int {
            val nToDrain = min(limit, precalculations.size)
            for (i in 1..nToDrain) {
                target.add(precalculations.poll() ?: throw RuntimeException("Queue must not be empty, race condition?"))
            }

            return nToDrain
        }

        /**
         * Calculates up to `limit` solutions (up to [Integer.MAX_VALUE]) and
         * adds them to the given collection.
         *
         * @param limit Maximum number of solutions to calculate. Must be less than [Integer.MAX_VALUE]
         * @return The number of solutions actually added. Is less than the requested
         *         limit if the solutions are depleted.
         */
        fun calculateSolutionsInto(target: MutableCollection<in Unification>, limit: Int): Int {
            if (limit >= Integer.MAX_VALUE) {
                throw IllegalArgumentException("Limit must be less than ${Integer.MAX_VALUE}")
            }

            var added = 0
            while (added < limit) {
                val solution = try {
                    solutions.tryAdvance()
                }
                catch (ex: PrologRuntimeException) {
                    throw QueryRelatedException(
                        QueryRelatedError(
                            queryId,
                            QueryRelatedError.Kind.ERROR_GENERIC,
                            ex.message,
                            mapOf("prologStackTrace" to ex.prettyPrint())
                        )
                    )
                }
                catch (ex: Throwable) {
                    throw NetworkProtocolException(
                        "Internal server error",
                        ex
                    )
                }

                if (solution == null) break

                target.add(solution)
                added++
            }

            return added
        }

        /**
         * Closes the context, releasing any open resources.
         */
        fun close() {
            TODO()
        }

        var precalculationLimit: Long
            get() = this@QueryContext.precalculationLimit
            set(value) {
                if (value < 0) throw IllegalArgumentException("Cannot set negative precalculation limiu")
                this@QueryContext.precalculationLimit = value
            }
    }
}

private fun PrologRuntimeException.prettyPrint(): String {
    val b = StringBuilder()
    prettyPrint(b)
    return b.toString()
}
private fun PrologRuntimeException.prettyPrint(toBuilder: StringBuilder) {
    toBuilder.append("M: ")
    toBuilder.append(message ?: "null")
    toBuilder.append("\n")
    for (sf in prologStackTrace) {
        toBuilder.append("\t")
        toBuilder.append(sf.toString())
        toBuilder.append("\n")
    }

    val _cause = cause
    if (_cause != null && _cause is PrologRuntimeException) {
        toBuilder.append("caused by: ")
        _cause.prettyPrint(toBuilder)
    }

    (suppressed
        .asSequence()
        .filter { it is PrologRuntimeException } as Sequence<PrologRuntimeException>)
        .forEach {
            toBuilder.append("suppressed: ")
            it.prettyPrint(toBuilder)
        }
}