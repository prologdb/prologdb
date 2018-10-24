package com.github.prologdb.net

import com.github.prologdb.async.LazySequence
import com.github.prologdb.net.session.*
import com.github.prologdb.net.session.handle.SessionHandle
import com.github.prologdb.net.util.prettyPrint
import com.github.prologdb.net.util.unsingedIntHexString
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.unification.Unification
import io.reactivex.rxkotlin.subscribeBy
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.channels.AsynchronousServerSocketChannel
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.concurrent.thread

class ServerInterface(
    /**
     * The instance MUST be thread-safe.
     */
    private val queryHandler: QueryHandler,

    /**
     * The number of worker threads to spawn. The worker threads will spawn the actual
     * queries through [QueryHandler.startQuery] and [QueryHandler.startDirective] as well
     * as calculate the solutions using [LazySequence.step].
     */
    private val nWorkerThreads: Int = Runtime.getRuntime().availableProcessors(),

    /**
     * Used to do the handshakes. This is where support for multiple protocols can be added.
     */
    private val sessionInitializer: SessionInitializer,

    /** The port to bind to. Use null to bind to an arbitrary available port. */
    givenPort: Int? = null
) {
    init {
        if (nWorkerThreads < 1) {
            throw IllegalArgumentException("The number of worker threads must be at least 1")
        }
        if (givenPort != null && (givenPort < 1 || givenPort > 65535)) {
            throw IllegalArgumentException("The port must be in range [1, 65535]")
        }
    }

    private val serverChannel = AsynchronousServerSocketChannel.open()
    init {
        serverChannel.bind(givenPort?.let { InetSocketAddress(it) })
    }

    val localAddress: SocketAddress
        get() = serverChannel.localAddress

    var closed: Boolean = false
        private set

    private val openSessions: MutableSet<SessionHandle> = Collections.newSetFromMap(ConcurrentHashMap())

    private val queryContexts: MutableMap<SessionHandle, MutableMap<Int, QueryContext>> = ConcurrentHashMap()

    private val accepterThread = thread {
        while (!closed) {
            val channel = try {
                serverChannel.accept().get()
            } catch (ex: Throwable) {
                // TODO: log
                continue
            }

            sessionInitializer.init(channel).subscribeBy(
                onSuccess= { handle ->
                    openSessions.add(handle)
                    handle.incomingMessages.subscribeBy(
                        onNext = { handleMessage(it, handle) },
                        onError = {
                            // TODO: log
                        },
                        onComplete = {
                            openSessions.remove(handle)
                        }
                    )
                },
                onError = {
                    // TODO: log
                    channel.close()
                }
            )
        }
    }

    private val workerZookeeperThread = thread {

    }

    private fun handleMessage(message: ProtocolMessage, handle: SessionHandle) {
        when (message) {
            is InitializeQueryCommand -> initializeQuery(message, handle)
            is ConsumeQuerySolutionsCommand -> consumeQuerySolutions(message, handle)
            is GeneralError -> handleClientError(message, handle)
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
        val queryContext = QueryContext(
            command.desiredQueryId,
            command.startUsing(queryHandler)
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

        context.doWith {
            it.registerConsumptionRequest(command)
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

    private inner class WorkerRunnable : Runnable, QueryContext.SolutionConsumptionListener {

        /**
         * Is set within [run] so that callbacks from [QueryContext] the
         * additional information that they need.
         */
        private var currentSessionHandle: SessionHandle? = null

        override fun run() {
            while (!closed) {
                for ((sessionHandle, contexts) in queryContexts) {
                    for (context in contexts.values) {
                        context.ifAvailable {
                            currentSessionHandle = sessionHandle
                            it.step()
                            it.consumeSolutions(this)
                            currentSessionHandle = null
                        }
                    }
                }
            }
        }

        override fun onReturnSolution(queryId: Int, solution: Unification) {
            currentSessionHandle!!.queueMessage(QuerySolutionMessage(queryId, solution))
        }

        override fun onSolutionsDepleted(queryId: Int) {
            currentSessionHandle!!.queueMessage(QueryClosedMessage(queryId, QueryClosedMessage.CloseReason.SOLUTIONS_DEPLETED))
        }

        override fun onAbortedByRequest(queryId: Int) {
            currentSessionHandle!!.queueMessage(QueryClosedMessage(queryId, QueryClosedMessage.CloseReason.ABORTED_ON_USER_REQUEST))
        }

        override fun onError(queryId: Int, ex: Throwable) {
            currentSessionHandle!!.queueMessage(QueryRelatedError(
                queryId,
                QueryRelatedError.Kind.ERROR_GENERIC,
                ex.message,
                if (ex is PrologRuntimeException) mapOf("prologStackTrace" to ex.prettyPrint()) else emptyMap()
            ))
            currentSessionHandle!!.queueMessage(QueryClosedMessage(queryId, QueryClosedMessage.CloseReason.FAILED))
        }
    }
}