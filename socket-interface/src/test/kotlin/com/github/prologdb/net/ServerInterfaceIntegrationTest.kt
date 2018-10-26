package com.github.prologdb.net

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.net.negotiation.ClientHello
import com.github.prologdb.net.negotiation.SemanticVersion
import com.github.prologdb.net.negotiation.ToClient
import com.github.prologdb.net.negotiation.ToServer
import com.github.prologdb.net.session.QueryHandler
import com.github.prologdb.net.session.SessionInitializer
import com.github.prologdb.net.session.handle.ProtocolVersion1SessionHandle
import com.github.prologdb.net.session.handle.ProtocolVersion1TermReader
import com.github.prologdb.net.session.handle.ProtocolVersion1TermWriter
import com.github.prologdb.net.session.handle.SessionHandle
import com.github.prologdb.net.v1.messages.QueryClosedEvent
import com.github.prologdb.net.v1.messages.QueryInitialization
import com.github.prologdb.net.v1.messages.QuerySolutionConsumption
import com.github.prologdb.parser.parser.PrologParser
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
                            ProtocolVersion1TermReader(
                                PrologParser(),
                                BinaryPrologReader.getDefaultInstance(),
                                DefaultOperatorRegistry()
                            ),
                            ProtocolVersion1TermWriter(
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

        override fun afterSpec(description: Description, spec: Spec) {
            interfaceInstance.close()
        }
    })

    init {
        "simple query" {
            val socket = Socket("localhost", interfaceInstance.localAddress.port)
            ToServer.newBuilder()
                .setHello(ClientHello.newBuilder()
                    .addDesiredProtocolVersion(ProtocolVersion1SemVer)
                    .build()
                )
                .build()
                .writeDelimitedTo(socket.getOutputStream())

            ToClient.parseDelimitedFrom(socket.getInputStream()).hello!!

            com.github.prologdb.net.v1.messages.ToServer.newBuilder()
                .setInitQuery(QueryInitialization.newBuilder()
                    .setKind(QueryInitialization.Kind.QUERY)
                    .setInstruction(com.github.prologdb.net.v1.messages.Term.newBuilder()
                        .setType(com.github.prologdb.net.v1.messages.Term.Type.STRING)
                        .setData(ByteString.copyFrom("foo(bar(Z)).", Charset.defaultCharset()))
                    )
                    .setQueryId(1)
                    .build()
                )
                .build()
                .writeDelimitedTo(socket.getOutputStream())

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
                .queryOpened

            queryOpened shouldNotBe null
            queryOpened.queryId shouldBe 1

            val solution = com.github.prologdb.net.v1.messages.ToClient.parseDelimitedFrom(socket.getInputStream())
                .solution

            solution shouldNotBe null
            solution.queryId shouldBe 1
            solution.instantiationsMap.size shouldBe 1
            val Avalue = solution.instantiationsMap["A"]
            Avalue!!
            val AvalueParsed = BinaryPrologReader.getDefaultInstance().readTermFrom(Avalue.data.asReadOnlyByteBuffer())
            AvalueParsed shouldBe Predicate("?-", arrayOf(PrologString("foo(bar(Z))")))

            val queryClosed = com.github.prologdb.net.v1.messages.ToClient.parseDelimitedFrom(socket.getInputStream())
                .queryClosed
            queryClosed.queryId shouldBe 1
            queryClosed.reason shouldBe QueryClosedEvent.Reason.SOLUTIONS_DEPLETED

            socket.close()
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
    override fun startQuery(query: Query, totalLimit: Long?): LazySequence<Unification> {
        return buildLazySequence(UUID.randomUUID()) {
            val vars = VariableBucket()
            vars.instantiate(Variable("A"), Predicate("?-", arrayOf(PrologString(query.toString()))))
            yield(Unification(vars))
        }
    }

    override fun startDirective(command: Predicate, totalLimit: Long?): LazySequence<Unification> {
        return buildLazySequence(UUID.randomUUID()) {
            val vars = VariableBucket()
            vars.instantiate(Variable("A"), Predicate(":-", arrayOf(command)))
            yield(Unification(vars))
        }
    }
}