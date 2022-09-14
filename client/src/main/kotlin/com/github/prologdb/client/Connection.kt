package com.github.prologdb.client

import com.github.prologdb.async.LazySequence
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.net.negotiation.ClientHello
import com.github.prologdb.net.negotiation.SemanticVersion
import com.github.prologdb.net.v1.messages.*
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.Unification
import com.google.protobuf.ByteString
import java.net.Socket
import kotlin.concurrent.thread
import com.github.prologdb.net.negotiation.ToClient as ToClientHS
import com.github.prologdb.net.negotiation.ToServer as ToServerHS
import com.github.prologdb.net.v1.messages.QueryClosedEvent as NetQClosedEvent
import com.github.prologdb.runtime.term.Term as RTTerm

class Connection(val host: String, val port: Int) {
    private val socket = Socket(host, port)

    private val mutex = Any()

    val serverVendor: String
    val serverVersion: SemanticVersion

    init {
        ToServerHS.newBuilder()
            .setHello(ClientHello.newBuilder().addDesiredProtocolVersion(PROTOCOL_VERSION1_SEMVER).build())
            .build()
            .writeDelimitedTo(socket.getOutputStream())

        val toClient = ToClientHS.parseDelimitedFrom(socket.getInputStream())
        if (toClient.error.isInitialized) {
            val error = PrologDBClientException(toClient.error.message)
            try {
                socket.close()
            }
            catch (ex: Throwable) {
                error.addSuppressed(error)
            }

            throw error
        }

        val serverHello = toClient.hello ?: throw RuntimeException("Server did not send a hello")
        serverVendor = serverHello.vendor
        serverVersion = serverHello.version
    }

    private val openQueries: MutableMap<Int, RemoteSolutions> = mutableMapOf()

    private var closed = false

    fun startQuery(instruction: String): LazySequence<Unification> = start(QueryInitialization.Kind.QUERY, instruction)
    fun startDirective(instruction: String): LazySequence<Unification> = start(QueryInitialization.Kind.DIRECTIVE, instruction)

    fun close() {
        closed = true

        ToServer.newBuilder()
            .setGoodbye(Goodbye.newBuilder())
            .build()
            .writeDelimitedTo(socket.getOutputStream())

        // give the reader some slack
        try { Thread.sleep(100) } catch (ex: InterruptedException) {}

        socket.close()
    }

    private fun start(kind: QueryInitialization.Kind, instruction: String): LazySequence<Unification> {
        synchronized(mutex) {
            val id = getNewQueryID()

            val command = QueryInitialization.newBuilder()
                .setKind(kind)
                .setInstruction(instruction.asNetworkQuery)
                .setQueryId(id)
                .build()

            ToServer.newBuilder()
                .setInitQuery(command)
                .build()
                .writeDelimitedTo(socket.getOutputStream())

            val solutionSeq = RemoteSolutions(id, 1, { amount -> this.requestSolutions(id, amount) }, { this.abort(id) })
            openQueries[id] = solutionSeq
            return solutionSeq
        }
    }

    private fun abort(queryId: Int) {
        synchronized(mutex) {
            ToServer.newBuilder()
                .setConsumeResults(QuerySolutionConsumption.newBuilder()
                    .setQueryId(queryId)
                    .setAmount(0)
                    .setHandling(QuerySolutionConsumption.PostConsumptionAction.DISCARD)
                    .setCloseAfterwards(true)
                    .build())
                .build()
                .writeDelimitedTo(socket.getOutputStream())
        }
    }

    private fun getNewQueryID(): Int {
        synchronized(mutex) {
            for (i in 0..Int.MAX_VALUE) {
                if (i !in openQueries) return i
            }

            throw IllegalStateException("No more query IDs available.")
        }
    }

    private fun requestSolutions(queryId: Int, amount: Int) {
        synchronized(mutex) {
            if (queryId !in openQueries) return
            if (openQueries[queryId]!!.closed) return

            ToServer.newBuilder()
                .setConsumeResults(QuerySolutionConsumption.newBuilder()
                    .setQueryId(queryId)
                    .setAmount(amount)
                    .setHandling(QuerySolutionConsumption.PostConsumptionAction.RETURN)
                    .setCloseAfterwards(false)
                    .build()
                )
                .build()
                .writeDelimitedTo(socket.getOutputStream())
        }
    }

    init {
        thread(start = true, name = "prologdb-conn-reader-$host-$port") {
            readMessage@while (!closed) {
                val toClient = ToClient.parseDelimitedFrom(socket.getInputStream())

                if (toClient == null) {
                    // EOF, Server closed connection
                    openQueries.forEach { (queryId, localSequence) ->
                        localSequence.onQueryEvent(QueryErrorEvent(QueryRelatedError.newBuilder()
                            .setQueryId(queryId)
                            .setKind(QueryRelatedError.Kind.ERROR_GENERIC)
                            .setShortMessage("Server closed connection")
                            .build()
                        ))
                        localSequence.onQueryEvent(QueryClosedEvent(NetQClosedEvent.Reason.FAILED))
                    }
                    break@readMessage
                }

                when (toClient.eventCase) {
                    ToClient.EventCase.SOLUTION -> {
                        val localSequence = openQueries[toClient.solution.queryId] ?: continue@readMessage
                        val vars = Unification()
                        for ((varName, value) in toClient.solution.instantiationsMap) {
                            vars.instantiate(Variable(varName), value.toRuntimeTerm())
                        }
                        localSequence.onQueryEvent(QuerySolutionEvent(Unification(vars)))
                    }
                    ToClient.EventCase.QUERY_OPENED -> {
                        // nothing to do
                    }
                    ToClient.EventCase.QUERY_CLOSED -> {
                        val localSequence = openQueries[toClient.queryClosed.queryId] ?: continue@readMessage
                        localSequence.onQueryEvent(QueryClosedEvent(toClient.queryClosed!!.reason))
                    }
                    ToClient.EventCase.QUERY_ERROR -> {
                        val localSequence = openQueries[toClient.queryError.queryId] ?: continue@readMessage
                        localSequence.onQueryEvent(QueryErrorEvent(toClient.queryError!!))
                    }
                    ToClient.EventCase.SERVER_ERROR -> {
                        TODO()
                    }
                    ToClient.EventCase.GOODBYE -> {
                        // nothing to do
                    }
                    ToClient.EventCase.EVENT_NOT_SET -> {
                        throw IllegalStateException("Received invalid data from server: missing event in ToClient")
                    }
                }
            }
        }
    }
}

private val PROTOCOL_VERSION1_SEMVER = SemanticVersion.newBuilder()
    .setMajor(1)
    .setMinor(0)
    .setPatch(0)
    .build()

private val String.asNetworkQuery: Query
    get() = Query.newBuilder()
        .setData(ByteString.copyFrom(this, Charsets.UTF_8))
        .setType(Query.Type.STRING)
        .build()

private fun Term.toRuntimeTerm(): RTTerm {
    return BinaryPrologReader.getDefaultInstance().readTermFrom(
        this.data.asReadOnlyByteBuffer()
    )
}