package com.github.prologdb.net

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.net.negotiation.*
import com.github.prologdb.net.session.QueryHandler
import com.github.prologdb.net.session.SessionInitializer
import com.github.prologdb.net.session.handle.ProtocolVersion1PrologReader
import com.github.prologdb.net.session.handle.ProtocolVersion1PrologWriter
import com.github.prologdb.net.session.handle.ProtocolVersion1SessionHandle
import com.github.prologdb.net.session.handle.SessionHandle
import com.github.prologdb.net.v1.messages.*
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.knowledge.library.DefaultOperatorRegistry
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.term.PrologString
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket
import com.google.protobuf.ByteString
import io.kotlintest.Description
import io.kotlintest.Spec
import io.kotlintest.extensions.TestListener
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.FreeSpec
import io.reactivex.subjects.SingleSubject
import java.lang.Math.ceil
import java.net.Socket
import java.nio.charset.Charset
import java.util.*
import com.github.prologdb.net.negotiation.ToClient as ToClientHS
import com.github.prologdb.net.negotiation.ToServer as ToServerHS

class ServerInterfaceIntegrationTest : FreeSpec() {

    private lateinit var interfaceInstance: ServerInterface

    override fun listeners(): List<TestListener> = listOf(object : TestListener {
        override fun beforeSpec(description: Description, spec: Spec) {
            interfaceInstance = ServerInterface(
                queryHandler,
                SessionInitializer("prologdb", serverSoftwareVersion, mapOf(
                    ProtocolVersion1SemVer to { channel, _ ->
                        val source = SingleSubject.create<SessionHandle>()
                        source.onSuccess(ProtocolVersion1SessionHandle(
                            channel,
                            ProtocolVersion1PrologReader(
                                PrologParser(),
                                BinaryPrologReader.getDefaultInstance(),
                                DefaultOperatorRegistry()
                            ),
                            ProtocolVersion1PrologWriter(
                                BinaryPrologWriter.getDefaultInstance()
                            )
                        ))
                        source
                    }
                )),
                null,
                { ceil(Runtime.getRuntime().availableProcessors().toDouble() / 4.0).toInt() }
            )
        }

        override fun beforeTest(description: Description) {
            queryHandler.errorOnQuery = false
            queryHandler.errorOnDirective = false
        }

        override fun afterSpec(description: Description, spec: Spec) {
            interfaceInstance.close()
        }
    })

    init {
        "simple query" {
            val (_, socket) = initConnection(interfaceInstance)

            socket.startQuery(1, "foo(bar(Z))")

            com.github.prologdb.net.v1.messages.ToServer.newBuilder()
                .setConsumeResults(QuerySolutionConsumption.newBuilder()
                    .setQueryId(1)
                    .setCloseAfterwards(false)
                    .setHandling(QuerySolutionConsumption.PostConsumptionAction.RETURN)
                    .build()
                )
                .build()
                .writeDelimitedTo(socket.getOutputStream())

            val queryOpened = com.github.prologdb.net.v1.messages.ToClient.parseDelimitedFrom(socket.getInputStream())
                .let {
                    it.eventCase shouldBe ToClient.EventCase.QUERY_OPENED
                    it.queryOpened
                }

            queryOpened shouldNotBe null
            queryOpened.isInitialized shouldBe true
            queryOpened.queryId shouldBe 1

            val solution = com.github.prologdb.net.v1.messages.ToClient.parseDelimitedFrom(socket.getInputStream())
                .let {
                    it.eventCase shouldBe ToClient.EventCase.SOLUTION
                    it.solution
                }

            solution shouldNotBe null
            solution.isInitialized shouldBe true
            solution.queryId shouldBe 1
            solution.instantiationsMap.size shouldBe 1
            val Avalue = solution.instantiationsMap["A"]
            Avalue!!
            val AvalueParsed = BinaryPrologReader.getDefaultInstance().readTermFrom(Avalue.data.asReadOnlyByteBuffer())
            AvalueParsed shouldBe Predicate("?-", arrayOf(PrologString("foo(bar(Z))")))

            val queryClosed = com.github.prologdb.net.v1.messages.ToClient.parseDelimitedFrom(socket.getInputStream())
                .let {
                    it.eventCase shouldBe ToClient.EventCase.QUERY_CLOSED
                    it.queryClosed
                }

            queryClosed.queryId shouldBe 1
            queryClosed.reason shouldBe QueryClosedEvent.Reason.SOLUTIONS_DEPLETED

            socket.close()
        }

        "duplicate query id" {
            /*val (_, socket) = initConnection(interfaceInstance)

            socket.startQuery(1, "foo(X).")

            val queryOpened = ToClient.parseDelimitedFrom(socket.getInputStream())
                .let {
                    it.eventCase shouldBe ToClient.EventCase.QUERY_OPENED
                    it.queryOpened
                }

            queryOpened.queryId shouldBe 1

            socket.startQuery(1, "bar(X).")

            val queryError = ToClient.parseDelimitedFrom(socket.getInputStream())
                .let {
                    it.eventCase shouldBe ToClient.EventCase.QUERY_ERROR
                    it.queryError
                }

            queryError.queryId shouldBe 1
            queryError.kind shouldBe QueryRelatedError.Kind.QUERY_ID_ALREADY_IN_USE

            socket.close()*/
        }

        "consume for query that was not initialized" {
            /*val (_, socket) = initConnection(interfaceInstance)

            ToServer.newBuilder()
                .setConsumeResults(QuerySolutionConsumption.newBuilder()
                    .setQueryId(2013)
                    .setHandling(QuerySolutionConsumption.PostConsumptionAction.RETURN)
                )
                .build()
                .writeDelimitedTo(socket.getOutputStream())

            val queryError = ToClient.parseDelimitedFrom(socket.getInputStream())
                .let {
                    it.eventCase shouldBe ToClient.EventCase.QUERY_ERROR
                    it.queryError
                }

            queryError shouldNotBe null
            queryError.queryId shouldBe 1
            queryError.kind shouldBe QueryRelatedError.Kind.QUERY_ID_NOT_IN_USE

            socket.close()*/
        }
    }
}

private val serverSoftwareVersion = SemanticVersion.newBuilder()
    .setMajor(0)
    .setMinor(1)
    .setPatch(0)
    .addPreReleaseLabels("SNAPSHOT")
    .build()

private val ProtocolVersion1SemVer = SemanticVersion.newBuilder()
    .setMajor(1)
    .setMinor(0)
    .setPatch(0)
    .addPreReleaseLabels("ALPHA")
    .build()

private val queryHandler = object : QueryHandler {

    var errorOnQuery: Boolean = false
    var errorOnDirective: Boolean = false

    override fun startQuery(term: Query, totalLimit: Long?): LazySequence<Unification> {
        return buildLazySequence(UUID.randomUUID()) {
            if (errorOnQuery) {
                throw PrologRuntimeException("Error :(")
            } else {
                val vars = VariableBucket()
                vars.instantiate(Variable("A"), Predicate("?-", arrayOf(PrologString(term.toString()))))
                yield(Unification(vars))
            }
        }
    }

    override fun startDirective(command: Predicate, totalLimit: Long?): LazySequence<Unification> {
        return buildLazySequence(UUID.randomUUID()) {
            if (errorOnDirective) {
                throw PrologRuntimeException("Error directive :(")
            } else {
                val vars = VariableBucket()
                vars.instantiate(Variable("A"), Predicate(":-", arrayOf(command)))
                yield(Unification(vars))
            }
        }
    }
}

private fun initConnection(serverInterface: ServerInterface): Pair<ServerHello, Socket> {
    val socket = Socket("localhost", serverInterface.localAddress.port)
    ToServerHS.newBuilder()
        .setHello(ClientHello.newBuilder()
            .addDesiredProtocolVersion(ProtocolVersion1SemVer)
            .build()
        )
        .build()
        .writeDelimitedTo(socket.getOutputStream())

    return Pair(ToClientHS.parseDelimitedFrom(socket.getInputStream()).hello!!, socket)
}

private fun Socket.startQuery(id: Int, query: String) {
    ToServer.newBuilder()
        .setInitQuery(QueryInitialization.newBuilder()
            .setKind(QueryInitialization.Kind.QUERY)
            .setInstruction(com.github.prologdb.net.v1.messages.Query.newBuilder()
                .setType(com.github.prologdb.net.v1.messages.Query.Type.STRING)
                .setData(ByteString.copyFrom(query, Charset.defaultCharset()))
            )
            .setQueryId(id)
            .build()
        )
        .build()
        .writeDelimitedTo(getOutputStream())
}

private fun Socket.startDirective(id: Int, command: String) {
    ToServer.newBuilder()
        .setInitQuery(QueryInitialization.newBuilder()
            .setKind(QueryInitialization.Kind.DIRECTIVE)
            .setInstruction(com.github.prologdb.net.v1.messages.Query.newBuilder()
                .setType(com.github.prologdb.net.v1.messages.Query.Type.STRING)
                .setData(ByteString.copyFrom(command, Charset.defaultCharset()))
            )
            .setQueryId(id)
            .build()
        )
        .build()
        .writeDelimitedTo(getOutputStream())
}