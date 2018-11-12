package com.github.prologdb.client

import com.github.prologdb.async.LazySequence
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.net.negotiation.ClientHello
import com.github.prologdb.net.negotiation.SemanticVersion
import com.github.prologdb.net.v1.messages.*
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket
import com.google.protobuf.ByteString
import java.net.Socket
import java.nio.charset.Charset
import kotlin.concurrent.thread
import com.github.prologdb.net.negotiation.ToClient as ToClientHS
import com.github.prologdb.net.negotiation.ToServer as ToServerHS
import com.github.prologdb.runtime.term.Term as RTTerm

class Connection(val host: String, val port: Int) {
    private val socket = Socket(host, port)

    private val mutex = Any()

    init {
        ToServerHS.newBuilder()
            .setHello(ClientHello.newBuilder().addDesiredProtocolVersion(PROTOCOL_VERSION1_SEMVER).build())
            .build()
            .writeDelimitedTo(socket.getOutputStream())

        val serverHello = ToClientHS.parseDelimitedFrom(socket.getInputStream())
        if (serverHello.error != null) {
            val error = PrologDBClientException(serverHello.error.message)
            try {
                socket.close()
            }
            catch (ex: Throwable) {
                error.addSuppressed(error)
            }

            throw error
        }
    }

    private val openQueries: MutableMap<Int, RemoteSolutions> = mutableMapOf()

    private var closed = false

    fun startQuery(instruction: String): LazySequence<Unification> = start(QueryInitialization.Kind.QUERY, instruction)
    fun startDirective(instruction: String): LazySequence<Unification> = start(QueryInitialization.Kind.DIRECTIVE, instruction)

    fun close() {
        // TODO
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

            val solutionStream = RemoteSolutions(id, { this.abort(id) })
            openQueries[id] = solutionStream

            return solutionStream
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

    private var queryIdCounter: Int = 1
    private fun getNewQueryID(): Int {
        synchronized(mutex) {
            var id: Int
            do {
                id = queryIdCounter++
            }
            while(id !in openQueries)

            return id
        }
    }

    init {
        thread(start = true, name = "prologdb-conn-reader-$host-$port") {
            readMessage@while (!closed) {
                val toClient = ToClient.parseDelimitedFrom(socket.getInputStream())
                when (toClient.eventCase) {
                    ToClient.EventCase.SOLUTION -> {
                        val localSequence = openQueries[toClient.solution.queryId] ?: continue@readMessage
                        val vars = VariableBucket()
                        for ((varName, value) in toClient.solution.instantiationsMap) {
                            vars.instantiate(Variable(varName), value.toRuntimeTerm())
                        }
                        localSequence.onSolution(Unification(vars))
                    }
                    ToClient.EventCase.QUERY_OPENED -> {
                        val localSequence = openQueries[toClient.queryOpened.queryId] ?: continue@readMessage
                        localSequence.onOpened()
                    }
                    ToClient.EventCase.QUERY_CLOSED -> {
                        val localSequence = openQueries[toClient.queryClosed.queryId] ?: continue@readMessage
                        localSequence.onClosed(toClient.queryClosed.reason)
                    }
                    ToClient.EventCase.QUERY_ERROR -> {
                        val localSequence = openQueries[toClient.queryError.queryId] ?: continue@readMessage
                        localSequence.onError(toClient.queryError)
                    }
                    ToClient.EventCase.SERVER_ERROR -> {
                        TODO()
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
        .setData(ByteString.copyFrom(this, Charset.defaultCharset()))
        .setType(Query.Type.STRING)
        .build()

private fun Term.toRuntimeTerm(): RTTerm {
    return BinaryPrologReader.getDefaultInstance().readTermFrom(
        this.data.asReadOnlyByteBuffer()
    )
}