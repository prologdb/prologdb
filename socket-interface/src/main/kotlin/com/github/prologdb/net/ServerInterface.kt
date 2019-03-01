package com.github.prologdb.net

import com.github.prologdb.async.LazySequence
import com.github.prologdb.net.session.*
import com.github.prologdb.net.session.handle.SessionHandle
import com.github.prologdb.net.util.prettyPrint
import com.github.prologdb.net.util.sortForSubstitution
import com.github.prologdb.net.util.unsingedIntHexString
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.query.PredicateInvocationQuery
import com.github.prologdb.runtime.unification.Unification
import io.reactivex.Single
import io.reactivex.rxkotlin.subscribeBy
import io.reactivex.subjects.SingleSubject
import org.slf4j.LoggerFactory
import java.lang.Math.floor
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetSocketAddress
import java.nio.channels.AsynchronousCloseException
import java.nio.channels.AsynchronousServerSocketChannel
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import kotlin.concurrent.thread

private val log = LoggerFactory.getLogger("prologdb.srv-intf")

class ServerInterface(
    /**
     * The instance MUST be thread-safe.
     */
    databaseEngine: DatabaseEngine<*>,

    /**
     * Used to do the handshakes. This is where support for multiple protocols can be added.
     */
    private val sessionInitializer: SessionInitializer,

    /** The port to bind to. Use null to bind to an arbitrary available port. */
    givenPort: Int? = null,

    /**
     * Provides the number of worker threads to maintain. Is consulted on a regular interval.
     * The worker threads will spawn the actual queries through [DatabaseEngine.startQuery] and
     * [DatabaseEngine.startDirective] as well as calculate the solutions using [LazySequence.step].
     */
    private val nWorkerThreadsSource: () -> Int = Runtime.getRuntime()::availableProcessors
) {
    init {
        if (givenPort != null && (givenPort < 1 || givenPort > 65535)) {
            throw IllegalArgumentException("The port must be in range [1, 65535]")
        }
    }

    private val databaseEngine: DatabaseEngine<Any?> = databaseEngine as DatabaseEngine<Any?>
    /* type erasure is okay since we'll only pass in what we get out of it. */

    private val serverChannel = AsynchronousServerSocketChannel.open()

    val localAddress: InetSocketAddress
        get() = serverChannel.localAddress as InetSocketAddress

    private val localAddressPretty: String
        get() {
            val localAddr = localAddress.address

            return if (localAddr is Inet4Address && localAddr.isAnyLocalAddress
                || localAddr is Inet6Address && localAddr.isAnyLocalAddress)
            {
                "*:" + localAddress.port
            } else {
                localAddress.toString()
            }
        }

    var closed: Boolean = false
        private set

    private val openSessions: MutableSet<SessionHandle> = Collections.newSetFromMap(ConcurrentHashMap())

    private val queryContexts: MutableMap<SessionHandle, MutableMap<Int, QueryContext>> = ConcurrentHashMap()

    private fun handleMessage(message: ProtocolMessage, handle: SessionHandle) {
        when (message) {
            is InitializeQueryCommand -> initializeQuery(message, handle)
            is ConsumeQuerySolutionsCommand -> consumeQuerySolutions(message, handle)
            is GeneralError -> handleClientError(message, handle)
            is ConnectionCloseEvent -> closeSession(handle)
            else -> throw IllegalArgumentException("Tasks with message of type ${message.javaClass.name} not supported")
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
        val initialized = try {
            command.startUsing(handle.sessionState, databaseEngine)
        }
        catch (ex: Throwable) {
            log.error("Unknown error while trying to start command ${command.instruction} as ${command.kind.name}", ex)

            handle.queueMessage(QueryRelatedError(
                command.desiredQueryId,
                QueryRelatedError.Kind.ERROR_GENERIC,
                (ex as? PrologRuntimeException)?.message ?: "Unknown Error",
                if (ex is PrologRuntimeException) mapOf("prologStackTrace" to ex.prettyPrint()) else emptyMap()
            ))
            handle.queueMessage(QueryClosedMessage(command.desiredQueryId, QueryClosedMessage.CloseReason.FAILED))
            return
        }

        val queryContext = QueryContext(
            command.desiredQueryId,
            initialized,
            command.instruction
        )

        // catch the race condition
        val queryIdPresent = contexts.putIfAbsent(command.desiredQueryId, queryContext) != null
        if (queryIdPresent) {
            refuseForDuplicateID()
        }

        // send query opened
        handle.queueMessage(QueryOpenedMessage(command.desiredQueryId))
    }

    private fun consumeQuerySolutions(command: ConsumeQuerySolutionsCommand, handle: SessionHandle) {
        val contexts = queryContexts.computeIfAbsent(handle) { ConcurrentHashMap() }
        val context = contexts[command.queryId]
        if (context == null) {
            handle.queueMessage(QueryRelatedError(
                command.queryId,
                QueryRelatedError.Kind.QUERY_ID_NOT_IN_USE,
                "Solutions were requested for not-initialized query ${command.queryId}"
            ))
            return
        }

        try {
            context.doWith {
                it.registerConsumptionRequest(command)
            }
        }
        catch (ex: QueryContextClosedException) { /* ignore */ }
    }

    private fun handleClientError(error: GeneralError, handle: SessionHandle) {
        val contexts = queryContexts.remove(handle)
        if (contexts != null) {
            contexts.values.forEach {
                it.doWith { it.close() }
            }
        }
        closeSession(handle)
    }

    private fun closeSession(session: SessionHandle) {
        session.closeSession()

    }

    private fun InitializeQueryCommand.startUsing(sessionState: Any?, engine: DatabaseEngine<Any?>): LazySequence<Unification> {
        if (kind == InitializeQueryCommand.Kind.DIRECTIVE && instruction !is PredicateInvocationQuery) {
            throw PrologRuntimeException("Directives must consist of a single predicate invocation query, found compound query.")
        }

        val instruction = if (preInstantiations == null || preInstantiations.isEmpty) instruction else {
            var _instruction = this.instruction
            val sortedSubstitutions = try {
                preInstantiations.sortForSubstitution()
            }
            catch (ex: PrologRuntimeException) {
                throw QueryRelatedException(
                    QueryRelatedError(
                        desiredQueryId,
                        QueryRelatedError.Kind.ERROR_GENERIC,
                        ex.message ?: "Failed to apply pre-instantiations"
                    ),
                    ex
                )
            }

            sortedSubstitutions.forEach {
                _instruction = _instruction.substituteVariables(it)
            }

            _instruction
        }

        return when (kind) {
            InitializeQueryCommand.Kind.QUERY     -> engine.startQuery(sessionState, instruction, totalLimit)
            InitializeQueryCommand.Kind.DIRECTIVE -> engine.startDirective(sessionState, (instruction as PredicateInvocationQuery).goal, totalLimit)
        }
    }

    private inner class WorkerRunnable : Runnable, QueryContext.SolutionConsumptionListener {

        /**
         * Is set within [run] so that callbacks from [QueryContext] the
         * additional information that they need.
         */
        private var currentSessionHandle: SessionHandle? = null

        private var closed = false

        /**
         * When closed gracefully through either [close] or [ServerInterface.closed],
         * completes. Emits an error that crashed the worker.
         */
        val completedEv: Single<Unit> = SingleSubject.create<Unit>()

        override fun run() {
            try {
                while (!this@ServerInterface.closed && !this@WorkerRunnable.closed) {
                    var nContextsWorkedOn = 0
                    for ((sessionHandle, contexts) in queryContexts) {
                        for (context in contexts.values) {
                            context.ifAvailable {
                                currentSessionHandle = sessionHandle
                                it.step()
                                it.consumeSolutions(this)
                                currentSessionHandle = null
                                nContextsWorkedOn++
                            }
                        }
                    }

                    if (nContextsWorkedOn == 0) {
                        // apparently there is nothing to do, wait a little
                        // as not to waste CPU time (between 200 and 800 ms)
                        Thread.sleep(floor(Math.random() * 600.0).toLong() + 200)
                    }
                }

                (completedEv as SingleSubject<Unit>).onSuccess(Unit)
            }
            catch (ex: Throwable) {
                ex.printStackTrace(System.err)
                (completedEv as SingleSubject<Unit>).onError(ex)
            }
        }

        fun close(): Single<Unit> {
            this@WorkerRunnable.closed = true

            return completedEv
        }

        override fun onReturnSolution(queryId: Int, solution: Unification) {
            // remove variables starting with an underscore that were present in the original query
            val original = queryContexts[currentSessionHandle]!![queryId]!!.originalQuery
            if (original.variables.any { it.name.startsWith('_') }) {
                val varsToRetain = solution.variableValues.variables
                    .filter { it !in original.variables || (it in original.variables && !it.name.startsWith('_')) }
                solution.variableValues.retainAll(varsToRetain)
            }

            currentSessionHandle!!.queueMessage(QuerySolutionMessage(queryId, solution))
        }

        override fun onSolutionsDepleted(queryId: Int) {
            currentSessionHandle!!.queueMessage(QueryClosedMessage(queryId, QueryClosedMessage.CloseReason.SOLUTIONS_DEPLETED))
        }

        override fun onAbortedByRequest(queryId: Int) {
            currentSessionHandle!!.queueMessage(QueryClosedMessage(queryId, QueryClosedMessage.CloseReason.ABORTED_ON_USER_REQUEST))
        }

        override fun onError(queryId: Int, ex: Throwable) {
            log.error("Query $queryId errored", ex)

            currentSessionHandle!!.queueMessage(QueryRelatedError(
                queryId,
                QueryRelatedError.Kind.ERROR_GENERIC,
                ex.message,
                if (ex is PrologRuntimeException) mapOf("prologStackTrace" to ex.prettyPrint()) else emptyMap()
            ))
            currentSessionHandle!!.queueMessage(QueryClosedMessage(queryId, QueryClosedMessage.CloseReason.FAILED))
        }
    }

    private val workers: MutableSet<Pair<WorkerRunnable, Thread>> = Collections.newSetFromMap(ConcurrentHashMap())

    private val workerZookeeper = object : TimerTask() {

        /** To keep the thread names for workers unique, this is an increment */
        private var workerIncrement: Long = 0

        override fun run() {
            try {
                val nDesiredWorkers = nWorkerThreadsSource()
                if (nDesiredWorkers < 1) {
                    log.warn("Desired number of workers is < 1.")
                    return
                }

                pruneDeadWorkers()
                while (nDesiredWorkers > workers.size) {
                    log.info("nDesiredWorkers = {}, nWorkers = {}, starting another", nDesiredWorkers, workers.size)
                    val runnable = WorkerRunnable()
                    val thread = Thread(runnable, "prologdb-interface-$localAddressPretty-worker-$workerIncrement")
                    workerIncrement++
                    workers.add(Pair(runnable, thread))
                    thread.start()
                    log.debug("Worker thread {} started", thread.name)
                }

                while (nDesiredWorkers < workers.size) {
                    log.info("nDesiredWorkers = {}, nWorkers = {}, stopping one", nDesiredWorkers, workers.size)
                    val entry = workers.first()
                    entry.first.close()
                    workers.remove(entry)
                    log.debug("Worker {} has been stopped.", entry.second.name)
                }
            }
            catch (ex: Throwable) {
                log.error("Exception while trying to scale worker threads", ex)
            }
        }

        private fun pruneDeadWorkers() {
            val dead = workers.filterNot { it.second.isAlive }
            workers.removeAll(dead)

            dead.forEach {
                it.first.completedEv.subscribeBy(
                    onError = { ex ->
                        log.error("Worker thread ${it.second.name} died because of an exception", ex)
                    },
                    onSuccess = { _ ->
                        // TODO: log
                        log.error("Worker thread ${it.second.name} stopped by itself (did not report an exception)")
                    }
                )
            }
        }
    }

    private val zookeeperTimer: Timer

    init {
        serverChannel.bind(givenPort?.let { InetSocketAddress(it) })

        zookeeperTimer = Timer("prologdb-interface-$localAddressPretty-worker-zookeeper")
        zookeeperTimer.scheduleAtFixedRate(workerZookeeper, 1000L, 10000)
    }

    private val accepterThread = thread(name = "prologdb-interface-$localAddressPretty-accepter") {
        while (!closed) {
            val channel = try {
                try {
                    serverChannel.accept().get()
                } catch (ex: ExecutionException) {
                    throw ex.cause ?: ex
                }
            }
            catch (ex: AsynchronousCloseException) {
                if (!closed) {
                    // server socket closed, but the interface is supposed to be open?
                    log.error("Server socket was closed unexpectedly.", ex)
                    // TODO: attempt to reopen??
                    break
                } else {
                    // interface closed, all good
                    continue
                }
            }
            catch (ex: Throwable) {
                log.info("Failed to accept client connection", ex)
                continue
            }

            try {
                Single.fromFuture(sessionInitializer.init(channel).toCompletableFuture())
                    .map {
                        Pair(it, this@ServerInterface.databaseEngine.initializeSession())
                    }
                    .subscribeBy(
                        onSuccess= { (handle, sessionState) ->
                            handle.sessionState = sessionState
                            openSessions.add(handle)

                            handle.incomingMessages.subscribeBy(
                                onNext = { handleMessage(it, handle) },
                                onError = { ex ->
                                    if (ex is QueryRelatedException) {
                                        handle.queueMessage(ex.errorObject)
                                    } else {
                                        // TODO: log properly
                                        ex.printStackTrace(System.err)
                                        handle.closeSession()
                                    }
                                },
                                onComplete = {
                                    openSessions.remove(handle)
                                    this@ServerInterface.databaseEngine.onSessionDestroyed(handle.sessionState)
                                }
                            )
                        },
                        onError = { ex ->
                            log.debug("Failed to negotiate session parameters with client, closing connection.", ex)
                            channel.close()
                        }
                    )
            }
            catch (ex: Throwable) {
                log.debug("Failed to negotiate session parameters with client, closing connection.", ex)
            }
        }
    }

    fun close() {
        closed = true
        zookeeperTimer.cancel()
        workers
            .map { it.first.close() }
            .forEach { it.blockingGet() }

        // TODO: close all running queries
        // TODO: notify clients!

        // this will also interrupt the accepterThread
        serverChannel.close()
    }
}
