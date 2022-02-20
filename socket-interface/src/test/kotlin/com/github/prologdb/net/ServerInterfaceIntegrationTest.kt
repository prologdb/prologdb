package com.github.prologdb.net

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.io.util.ByteArrayOutputStream
import com.github.prologdb.net.negotiation.ClientHello
import com.github.prologdb.net.negotiation.SemanticVersion
import com.github.prologdb.net.negotiation.ServerError
import com.github.prologdb.net.negotiation.ServerHello
import com.github.prologdb.net.session.DatabaseEngine
import com.github.prologdb.net.session.SessionInitializer
import com.github.prologdb.net.session.handle.IsoOpsStatelessParserDelegate
import com.github.prologdb.net.session.handle.ProtocolVersion1SessionHandle
import com.github.prologdb.net.session.handle.SessionHandle
import com.github.prologdb.net.v1.messages.*
import com.github.prologdb.parser.parser.ParseResult
import com.github.prologdb.parser.source.SourceUnit
import com.github.prologdb.runtime.PrologInternalError
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.PrologString
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket
import com.google.protobuf.ByteString
import io.kotlintest.*
import io.kotlintest.extensions.TestListener
import io.kotlintest.matchers.collections.contain
import io.kotlintest.specs.FreeSpec
import java.lang.Math.ceil
import java.net.Socket
import java.nio.channels.AsynchronousByteChannel
import java.nio.charset.Charset
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import com.github.prologdb.net.negotiation.ToClient as ToClientHS
import com.github.prologdb.net.negotiation.ToServer as ToServerHS

class ServerInterfaceIntegrationTest : FreeSpec() {

    private lateinit var interfaceInstance: ServerInterface<Map<String, String>>

    override fun listeners(): List<TestListener> = listOf(object : TestListener {
        override fun beforeSpec(description: Description, spec: Spec) {
            interfaceInstance = ServerInterface(
                queryHandler,
                SessionInitializer("prologdb", serverSoftwareVersion, mapOf(
                    ProtocolVersion1SemVer to ProtoclVersion1HandleFactory
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
        "handshake" - {
            "given client wants unsupported protocol: server responds with error" {
                val versionNotSupported = SemanticVersion.newBuilder()
                    .setMajor(0)
                    .setMinor(5)
                    .setPatch(0)
                    .addPreReleaseLabels("BETA")

                val socket = Socket("localhost", interfaceInstance.localAddress.port)
                ToServerHS.newBuilder()
                    .setHello(ClientHello.newBuilder()
                        .addDesiredProtocolVersion(versionNotSupported)
                        .build()
                    )
                    .build()
                    .writeDelimitedTo(socket.getOutputStream())

                val toClientHS = ToClientHS.parseDelimitedFrom(socket.getInputStream())
                toClientHS.messageCase shouldBe com.github.prologdb.net.negotiation.ToClient.MessageCase.ERROR
                val error = toClientHS.error
                error.kind shouldBe ServerError.Kind.GENERIC
            }

            "given server supports newer protocols than client: server chooses latest of client" {
                // SETUP
                val versionOneDotTwo = SemanticVersion.newBuilder()
                    .setMajor(1)
                    .setMinor(2)
                    .setPatch(0)
                    .build()

                val versionOneDotThree = SemanticVersion.newBuilder()
                    .setMajor(1)
                    .setMinor(3)
                    .setPatch(0)
                    .build()

                // assure that this test case makes sens
                assert(versionOneDotTwo.major >= ProtocolVersion1SemVer.major)
                assert(versionOneDotTwo.minor > ProtocolVersion1SemVer.minor)
                assert(versionOneDotTwo.patch >= ProtocolVersion1SemVer.patch)

                val secondInterface = ServerInterface(
                    queryHandler,
                    SessionInitializer("prologdb", serverSoftwareVersion, mapOf(
                        ProtocolVersion1SemVer to ProtoclVersion1HandleFactory,
                        versionOneDotTwo to ProtoclVersion1HandleFactory,
                        versionOneDotThree to ProtoclVersion1HandleFactory
                    )),
                    null,
                    { ceil(Runtime.getRuntime().availableProcessors().toDouble() / 4.0).toInt() }
                )

                // ACT
                try {
                    Socket("localhost", secondInterface.localAddress.port).use { socket ->
                        ToServerHS.newBuilder()
                            .setHello(ClientHello.newBuilder()
                                .addDesiredProtocolVersion(ProtocolVersion1SemVer)
                                .addDesiredProtocolVersion(versionOneDotTwo)
                                .build()
                            )
                            .build()
                            .writeDelimitedTo(socket.getOutputStream())

                        val toClientHS = ToClientHS.parseDelimitedFrom(socket.getInputStream())
                        toClientHS.messageCase shouldBe com.github.prologdb.net.negotiation.ToClient.MessageCase.HELLO
                        toClientHS.hello.chosenProtocolVersion shouldBe versionOneDotTwo
                        toClientHS.hello.supportedProtocolVersionsList should contain(ProtocolVersion1SemVer)
                        toClientHS.hello.supportedProtocolVersionsList should contain(versionOneDotThree)
                    }
                }
                finally {
                    secondInterface.close()
                }
            }

            "given client does not specify a protocol: server chooses latest supported" {
                // SETUP
                val versionOneDotTwo = SemanticVersion.newBuilder()
                    .setMajor(1)
                    .setMinor(2)
                    .setPatch(0)
                    .build()

                // assure that this test case makes sens
                assert(versionOneDotTwo.major >= ProtocolVersion1SemVer.major)
                assert(versionOneDotTwo.minor > ProtocolVersion1SemVer.minor)
                assert(versionOneDotTwo.patch >= ProtocolVersion1SemVer.patch)

                val secondInterface = ServerInterface(
                    queryHandler,
                    SessionInitializer("prologdb", serverSoftwareVersion, mapOf(
                        ProtocolVersion1SemVer to ProtoclVersion1HandleFactory,
                        versionOneDotTwo to ProtoclVersion1HandleFactory
                    )),
                    null,
                    { ceil(Runtime.getRuntime().availableProcessors().toDouble() / 4.0).toInt() }
                )

                // ACT
                try {
                    Socket("localhost", secondInterface.localAddress.port).use { socket ->
                        ToServerHS.newBuilder()
                            .setHello(ClientHello.newBuilder()
                                .clearDesiredProtocolVersion()
                                .build()
                            )
                            .build()
                            .writeDelimitedTo(socket.getOutputStream())

                        val toClientHS = ToClientHS.parseDelimitedFrom(socket.getInputStream())
                        toClientHS.messageCase shouldBe com.github.prologdb.net.negotiation.ToClient.MessageCase.HELLO
                        toClientHS.hello.chosenProtocolVersion shouldBe versionOneDotTwo
                        toClientHS.hello.supportedProtocolVersionsList should contain(ProtocolVersion1SemVer)
                    }
                }
                finally {
                    secondInterface.close()
                }
            }

            "given invalid protobuf: handshake errors" {
                val socket = Socket("localhost", interfaceInstance.localAddress.port)

                val messageOut = ByteArrayOutputStream()
                ToServerHS.newBuilder()
                    .setHello(ClientHello.newBuilder()
                        .addDesiredProtocolVersion(ProtocolVersion1SemVer)
                        .build()
                    )
                    .build()
                    .writeDelimitedTo(messageOut)

                messageOut.bufferOfData.position(0)
                messageOut.bufferOfData.put(21)
                messageOut.bufferOfData.put(-121)
                messageOut.bufferOfData.put(30)
                messageOut.bufferOfData.put(1)
                messageOut.bufferOfData.put(-100)
                messageOut.bufferOfData.put(60)
                messageOut.bufferOfData.put(120)
                messageOut.bufferOfData.put(10)
                messageOut.bufferOfData.put(52)
                messageOut.bufferOfData.put(73)

                socket.getOutputStream().write(messageOut.bufferOfData.array())

                val toClientHS = ToClientHS.parseDelimitedFrom(socket.getInputStream())
                socket.close()

                toClientHS.messageCase shouldBe com.github.prologdb.net.negotiation.ToClient.MessageCase.ERROR
                toClientHS.error.kind shouldBe ServerError.Kind.INVALID_WIRE_FORMAT
            }
        }

        "query" - {
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
                AvalueParsed shouldBe CompoundTerm("?-", arrayOf(PrologString("foo(bar(Z))")))

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
                val (_, socket) = initConnection(interfaceInstance)

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

                socket.close()
            }

            "consume for query that was not initialized" {
                val (_, socket) = initConnection(interfaceInstance)

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
                queryError.queryId shouldBe 2013
                queryError.kind shouldBe QueryRelatedError.Kind.QUERY_ID_NOT_IN_USE

                socket.close()
            }

            "pre-instantiations" {
                val (_, socket) = initConnection(interfaceInstance)

                socket.startQuery(1, "foo(X)", mapOf("Y" to "X", "X" to "5587"))

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
                AvalueParsed shouldBe CompoundTerm("?-", arrayOf(PrologString("foo(5587)")))

                val queryClosed = com.github.prologdb.net.v1.messages.ToClient.parseDelimitedFrom(socket.getInputStream())
                    .let {
                        it.eventCase shouldBe ToClient.EventCase.QUERY_CLOSED
                        it.queryClosed
                    }

                queryClosed.queryId shouldBe 1
                queryClosed.reason shouldBe QueryClosedEvent.Reason.SOLUTIONS_DEPLETED

                socket.close()
            }

            "invalid binary prolog in query" {
                val (_, socket) = initConnection(interfaceInstance)

                ToServer.newBuilder()
                    .setInitQuery(QueryInitialization.newBuilder()
                        .setQueryId(2)
                        .setInstruction(com.github.prologdb.net.v1.messages.Query.newBuilder()
                            .setType(com.github.prologdb.net.v1.messages.Query.Type.BINARY)
                            .setData(ByteString.copyFrom("not valid binary prolog", Charset.defaultCharset()))
                            .build()
                        )
                        .build()
                    )
                    .build()
                    .writeDelimitedTo(socket.getOutputStream())

                val toClient = ToClient.parseDelimitedFrom(socket.getInputStream())
                socket.close()

                toClient.eventCase shouldBe ToClient.EventCase.QUERY_ERROR
                toClient.queryError.kind shouldBe QueryRelatedError.Kind.INVALID_TERM_SYNTAX
                toClient.queryError.queryId shouldBe 2
            }

            "invalid binary prolog in pre-instantiation" {
                val (_, socket) = initConnection(interfaceInstance)

                ToServer.newBuilder()
                    .setInitQuery(QueryInitialization.newBuilder()
                        .setQueryId(2)
                        .setInstruction(com.github.prologdb.net.v1.messages.Query.newBuilder()
                            .setType(com.github.prologdb.net.v1.messages.Query.Type.STRING)
                            .setData(ByteString.copyFrom("foo(X)", Charset.defaultCharset()))
                            .build()
                        )
                        .putInstantiations("X", Term.newBuilder()
                            .setType(Term.Type.BINARY)
                            .setData(ByteString.copyFrom("not valid binary prolog", Charset.defaultCharset()))
                            .build()
                        )
                        .build()
                    )
                    .build()
                    .writeDelimitedTo(socket.getOutputStream())

                val toClient = ToClient.parseDelimitedFrom(socket.getInputStream())
                socket.close()

                toClient.eventCase shouldBe ToClient.EventCase.QUERY_ERROR
                toClient.queryError.kind shouldBe QueryRelatedError.Kind.INVALID_TERM_SYNTAX
                toClient.queryError.queryId shouldBe 2
            }
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

private val queryHandler = object : DatabaseEngine<Map<String, String>> {

    var errorOnQuery: Boolean = false
    var errorOnDirective: Boolean = false

    override fun initializeSession(): Map<String, String> {
        return mutableMapOf()
    }

    override fun onSessionDestroyed(state: Map<String, String>) {

    }

    override fun startQuery(session: Map<String, String>, query: Query, totalLimit: Long?): LazySequence<Unification> {
        return buildLazySequence(UUID.randomUUID()) {
            if (errorOnQuery) {
                throw PrologInternalError("Error :(")
            } else {
                val vars = VariableBucket()
                vars.instantiate(Variable("A"), CompoundTerm("?-", arrayOf(PrologString(query.toString()))))
                Unification(vars)
            }
        }
    }

    override fun startDirective(session: Map<String, String>, command: CompoundTerm, totalLimit: Long?): LazySequence<Unification> {
        return buildLazySequence(UUID.randomUUID()) {
            if (errorOnDirective) {
                throw PrologInternalError("Error directive :(")
            } else {
                val vars = VariableBucket()
                vars.instantiate(Variable("A"), CompoundTerm(":-", arrayOf(command)))
                Unification(vars)
            }
        }
    }

    override fun parseTerm(context: Map<String, String>?, codeToParse: String, origin: SourceUnit): ParseResult<com.github.prologdb.runtime.term.Term> {
        TODO()
    }

    override fun parseQuery(context: Map<String, String>?, codeToParse: String, origin: SourceUnit): ParseResult<Query> {
        TODO()
    }
}

private fun initConnection(serverInterface: ServerInterface<*>): Pair<ServerHello, Socket> {
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

private fun Socket.startQuery(id: Int, query: String, instantiations: Map<String, String> = emptyMap()) {
    val initBuilder = QueryInitialization.newBuilder()
        .setKind(QueryInitialization.Kind.QUERY)
        .setInstruction(com.github.prologdb.net.v1.messages.Query.newBuilder()
            .setType(com.github.prologdb.net.v1.messages.Query.Type.STRING)
            .setData(ByteString.copyFrom(query, Charset.defaultCharset()))
        )
        .setQueryId(id)

    for ((varName, termText) in instantiations) {
        initBuilder.putInstantiations(varName, Term.newBuilder()
            .setType(Term.Type.STRING)
            .setData(ByteString.copyFrom(termText, Charset.defaultCharset()))
            .build()
        )
    }

    ToServer.newBuilder()
        .setInitQuery(initBuilder.build())
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

private val ProtoclVersion1HandleFactory: (AsynchronousByteChannel, ClientHello) -> CompletionStage<SessionHandle<*>> = { channel, _ ->
    val future = CompletableFuture<SessionHandle<*>>()
    future.complete(ProtocolVersion1SessionHandle(
        UUID.randomUUID().toString(),
        channel,
        IsoOpsStatelessParserDelegate,
        BinaryPrologReader.getDefaultInstance(),
        BinaryPrologWriter.getDefaultInstance()
    ))
    future
}
