package com.github.prologdb.net.session.handle

import com.github.prologdb.io.binaryprolog.BinaryPrologDeserializationException
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.io.util.ByteArrayOutputStream
import com.github.prologdb.net.PrologDeserializationException
import com.github.prologdb.net.PrologParseException
import com.github.prologdb.net.QueryRelatedException
import com.github.prologdb.net.async.AsyncByteChannelDelimitedProtobufReader
import com.github.prologdb.net.async.AsyncChannelProtobufOutgoingQueue
import com.github.prologdb.net.async.PipeClosedException
import com.github.prologdb.net.async.writeDelimitedTo
import com.github.prologdb.net.session.*
import com.github.prologdb.net.v1.messages.*
import com.github.prologdb.net.v1.messages.GeneralError
import com.github.prologdb.net.v1.messages.QueryRelatedError
import com.github.prologdb.net.v1.messages.ToClient
import com.github.prologdb.net.v1.messages.ToServer
import com.github.prologdb.parser.source.SourceUnit
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.VariableBucket
import com.google.protobuf.ByteString
import com.google.protobuf.GeneratedMessageV3
import io.reactivex.Observable
import io.reactivex.subjects.PublishSubject
import org.slf4j.LoggerFactory
import java.io.DataOutputStream
import java.nio.channels.AsynchronousByteChannel
import java.util.*
import java.util.concurrent.Callable
import java.util.function.Consumer

private val log = LoggerFactory.getLogger("prologdb.network")

internal class ProtocolVersion1SessionHandle(
    override val clientId: String,
    private val channel: AsynchronousByteChannel,
    parserDelegate: ParserDelegate<*>,
    binaryReader: BinaryPrologReader,
    binaryWriter: BinaryPrologWriter
) : SessionHandle {
    override var sessionState: Any? = null

    override val incomingMessages: Observable<ProtocolMessage>

    private val prologReader = ProtocolVersion1PrologReader(
        parserDelegate as ParserDelegate<Any?>,
        binaryReader
    )

    private val prologWriter = ProtocolVersion1PrologWriter(binaryWriter)

    init {
        val incomingVersionMessages = PublishSubject.create<ToServer>()
        AsyncByteChannelDelimitedProtobufReader(
            ToServer::class.java,
            channel,
            Consumer { incomingVersionMessages.onNext(it) },
            Consumer { incomingVersionMessages.onError(it) },
            Callable<Unit> { incomingVersionMessages.onComplete() }
        )

        incomingMessages = incomingVersionMessages
            .doOnEach { it ->
                if (it.isOnNext && it.value!!.commandCase == ToServer.CommandCase.GOODBYE) {
                    closeSession()
                }
            }
            .map { versionMessage ->
                when (versionMessage.commandCase!!) {
                    ToServer.CommandCase.CONSUME_RESULTS -> Optional.of(versionMessage.consumeResults.toIndependent())
                    ToServer.CommandCase.INIT_QUERY ->
                        try {
                            Optional.of(versionMessage.initQuery.toIndependent(sessionState, prologReader))
                        }
                        catch (ex: Throwable) {
                            log.debug("Got error while trying to read query", ex)

                            val kind: com.github.prologdb.net.session.QueryRelatedError.Kind
                            val additional = mutableMapOf<String, String>()

                            when(ex) {
                                is BinaryPrologDeserializationException,
                                is PrologDeserializationException -> {
                                    kind = com.github.prologdb.net.session.QueryRelatedError.Kind.INVALID_TERM_SYNTAX
                                }
                                is PrologParseException -> {
                                    kind = com.github.prologdb.net.session.QueryRelatedError.Kind.INVALID_TERM_SYNTAX
                                    ex.errors.forEachIndexed { index, reporting ->
                                        additional["error_$index"] = "${reporting.message} in ${reporting.location}"
                                    }
                                }
                                else -> {
                                    kind = com.github.prologdb.net.session.QueryRelatedError.Kind.ERROR_GENERIC
                                }
                            }

                            outQueue.queue(com.github.prologdb.net.session.QueryRelatedError(
                                versionMessage.initQuery.queryId,
                                kind,
                                ex.message,
                                additional
                            ).toProtocol())
                            Optional.empty<ProtocolMessage>()
                        }
                    ToServer.CommandCase.ERROR -> Optional.of(versionMessage.error.toIndependent())
                    ToServer.CommandCase.COMMAND_NOT_SET -> {
                        outQueue.queue(com.github.prologdb.net.session.GeneralError(
                            "command field of ToServer message not set",
                            mapOf("erroneousMessage" to versionMessage.toString())
                        ).toProtocol())
                        Optional.empty()
                    }
                    // this should have been handled earlier
                    ToServer.CommandCase.GOODBYE -> Optional.of(ConnectionCloseEvent())
                }
            }
            .filter { it.isPresent }
            .map { it.get() }
            .doOnError { ex ->
                if (ex is QueryRelatedException) {
                    ex.errorObject.toProtocol().writeDelimitedTo(channel)
                } else {
                    ToClient.newBuilder()
                        .setServerError(GeneralError.newBuilder()
                            .setMessage(ex.message ?: "")
                            .build()
                        )
                        .build()
                        .writeDelimitedTo(channel)

                    closeSession()
                }
            }
    }

    private val outQueue = AsyncChannelProtobufOutgoingQueue(channel)

    override fun queueMessage(message: ProtocolMessage) {
        if (message.javaClass.getAnnotation(com.github.prologdb.net.session.ToClient::class.java) == null) {
            throw IllegalArgumentException("Can only send messages intended for the client; see the ToClient annotation")
        }

        outQueue.queue(message.toProtocol(prologWriter))
    }

    override fun closeSession() {
        try {
            outQueue.queue(ToClient.newBuilder()
                .setGoodbye(Goodbye.getDefaultInstance())
                .build()
            )
        }
        catch (ex: PipeClosedException) {}

        outQueue.close()
        channel.close()
    }
}

private class ProtocolVersion1PrologReader(
    private val parser: ParserDelegate<Any?>,
    private val binaryPrologReader: BinaryPrologReader
) {
    fun toRuntimeTerm(sessionState: Any?, protoTerm: com.github.prologdb.net.v1.messages.Term, source: SourceUnit): Term {
        return when (protoTerm.type!!) {
            com.github.prologdb.net.v1.messages.Term.Type.BINARY -> {
                try {
                    binaryPrologReader.readTermFrom(protoTerm.data.asReadOnlyByteBuffer())
                }
                catch (ex: BinaryPrologDeserializationException) {
                    throw PrologDeserializationException("Failed to read binary prolog from $source", ex)
                }
            }
            com.github.prologdb.net.v1.messages.Term.Type.STRING -> {
                val result = parser.parseTerm(sessionState, protoTerm.data.toStringUtf8(), source)
                if (result.isSuccess) {
                    result.item!!
                } else {
                    throw PrologParseException(result)
                }
            }
        }
    }

    fun toRuntimeQuery(sessionState: Any?, protoQuery: com.github.prologdb.net.v1.messages.Query, source: SourceUnit): Query {
        return when (protoQuery.type!!) {
            com.github.prologdb.net.v1.messages.Query.Type.BINARY -> {
                try {
                    binaryPrologReader.readQueryFrom(protoQuery.data.asReadOnlyByteBuffer())
                }
                catch (ex: BinaryPrologDeserializationException) {
                    throw PrologDeserializationException(ex.message ?: "Failed to read binary prolog from $source", ex)
                }
            }
            com.github.prologdb.net.v1.messages.Query.Type.STRING -> {
                val result = parser.parseQuery(sessionState, protoQuery.data.toStringUtf8(), source)
                if (result.isSuccess) {
                    result.item!!
                } else {
                    throw PrologParseException(result)
                }
            }
        }
    }
}

private class ProtocolVersion1PrologWriter(
    private val binaryPrologWriter: BinaryPrologWriter
) {
    private val buffer = ThreadLocal.withInitial { val stream = ByteArrayOutputStream(512); Pair(stream, DataOutputStream(stream)) }

    fun write(term: Term): com.github.prologdb.net.v1.messages.Term {
        val (bufferStream, dataOut) = buffer.get()
        bufferStream.reset()
        binaryPrologWriter.writeTermTo(term, dataOut)

        return com.github.prologdb.net.v1.messages.Term.newBuilder()
            .setType(com.github.prologdb.net.v1.messages.Term.Type.BINARY)
            .setData(ByteString.copyFrom(bufferStream.bufferOfData))
            .build()
    }
}

private val SOURCE_UNIT_QUERY = SourceUnit("query")
private val SOURCE_UNIT_DIRECTIVE = SourceUnit("directive")

private fun QueryInitialization.toIndependent(sessionState: Any?, prologReader: ProtocolVersion1PrologReader): InitializeQueryCommand {
    val cmd = InitializeQueryCommand(
        queryId,
        prologReader.toRuntimeQuery(
            sessionState,
            instruction,
            when(kind!!) {
                QueryInitialization.Kind.QUERY -> SOURCE_UNIT_QUERY
                QueryInitialization.Kind.DIRECTIVE -> SOURCE_UNIT_DIRECTIVE
            }
        ),
        when(instantiationsCount) {
            0 -> null
            else -> instantiationsMap.toBucket(sessionState, prologReader)
        },
        kind.toIndependent(),
        if (hasLimit()) limit else null
    )

    return cmd
}

private fun Map<String, com.github.prologdb.net.v1.messages.Term>.toBucket(sessionState: Any?, prologReader: ProtocolVersion1PrologReader): VariableBucket {
    val bucket = VariableBucket()
    for ((variableName, term) in this) {
        bucket.instantiate(Variable(variableName), prologReader.toRuntimeTerm(sessionState, term, SourceUnit("parameter $variableName")))
    }

    return bucket
}

private fun QueryInitialization.Kind.toIndependent() = when(this) {
    QueryInitialization.Kind.DIRECTIVE -> InitializeQueryCommand.Kind.DIRECTIVE
    QueryInitialization.Kind.QUERY -> InitializeQueryCommand.Kind.QUERY
}

private fun QuerySolutionConsumption.toIndependent() = ConsumeQuerySolutionsCommand(
    queryId,
    if (hasAmount()) amount else null,
    closeAfterwards,
    handling.toIndependent()
)

private fun QuerySolutionConsumption.PostConsumptionAction.toIndependent() = when(this) {
    QuerySolutionConsumption.PostConsumptionAction.DISCARD -> ConsumeQuerySolutionsCommand.SolutionHandling.DISCARD
    QuerySolutionConsumption.PostConsumptionAction.RETURN-> ConsumeQuerySolutionsCommand.SolutionHandling.RETURN
}

private fun GeneralError.toIndependent() = com.github.prologdb.net.session.GeneralError(
    message,
    additionalInformationMap
)

private fun ProtocolMessage.toProtocol(prologWriter: ProtocolVersion1PrologWriter): GeneratedMessageV3 = when(this) {
    is com.github.prologdb.net.session.GeneralError -> toProtocol()
    is QueryOpenedMessage -> toProtocol()
    is QueryClosedMessage -> toProtocol()
    is com.github.prologdb.net.session.QueryRelatedError -> toProtocol()
    is QuerySolutionMessage -> toProtocol(prologWriter)
    else -> throw IllegalArgumentException("Cannot convert message of type ${this.javaClass.name} to protocol version 1 because that is not a to-client message")
}

private fun QueryOpenedMessage.toProtocol() = ToClient.newBuilder()
    .setQueryOpened(
        QueryOpenedEvent.newBuilder()
        .setQueryId(queryId)
        .build()
    )
    .build()

private fun QueryClosedMessage.CloseReason.toProtocol() = when(this) {
    QueryClosedMessage.CloseReason.SOLUTIONS_DEPLETED -> QueryClosedEvent.Reason.SOLUTIONS_DEPLETED
    QueryClosedMessage.CloseReason.ABORTED_ON_USER_REQUEST -> QueryClosedEvent.Reason.ABORTED_ON_USER_REQUEST
    QueryClosedMessage.CloseReason.FAILED -> QueryClosedEvent.Reason.FAILED
}

private fun QueryClosedMessage.toProtocol() = ToClient.newBuilder()
    .setQueryClosed(
        QueryClosedEvent.newBuilder()
        .setQueryId(queryId)
        .setReason(reason.toProtocol())
        .build()
    )
    .build()

private fun com.github.prologdb.net.session.GeneralError.toProtocol() = ToClient.newBuilder()
    .setServerError(
        GeneralError.newBuilder()
        .setMessage(message)
        .putAllAdditionalInformation(additionalInformation)
        .build()
    )
    .build()

private fun com.github.prologdb.net.session.QueryRelatedError.toProtocol() = ToClient.newBuilder()
    .setQueryError(
        QueryRelatedError.newBuilder()
        .setQueryId(queryId)
        .setKind(kind.toProtocol())
        .setShortMessage(shortMessage ?: "No Message.")
        .putAllAdditionalInformation(additionalFields)
        .build()
    )
    .build()

private fun com.github.prologdb.net.session.QueryRelatedError.Kind.toProtocol() = when(this) {
    com.github.prologdb.net.session.QueryRelatedError.Kind.QUERY_ID_NOT_IN_USE -> QueryRelatedError.Kind.QUERY_ID_NOT_IN_USE
    com.github.prologdb.net.session.QueryRelatedError.Kind.ERROR_CONSTRAINT_VIOLATION -> QueryRelatedError.Kind.ERROR_CONSTRAINT_VIOLATION
    com.github.prologdb.net.session.QueryRelatedError.Kind.ERROR_GENERIC -> QueryRelatedError.Kind.ERROR_GENERIC
    com.github.prologdb.net.session.QueryRelatedError.Kind.ERROR_PREDICATE_NOT_DYNAMIC -> QueryRelatedError.Kind.ERROR_PREDICATE_NOT_DYNAMIC
    com.github.prologdb.net.session.QueryRelatedError.Kind.INVALID_TERM_SYNTAX -> QueryRelatedError.Kind.INVALID_TERM_SYNTAX
    com.github.prologdb.net.session.QueryRelatedError.Kind.QUERY_ID_ALREADY_IN_USE -> QueryRelatedError.Kind.QUERY_ID_ALREADY_IN_USE
}

private fun QuerySolutionMessage.toProtocol(prologWriter: ProtocolVersion1PrologWriter): ToClient {
    val builder = QuerySolution.newBuilder()
        .setQueryId(queryId)

    solution.variableValues.values.forEach {
        val term = it.second
        if (term != null) {
            builder.putInstantiations(it.first.name, prologWriter.write(term))
        }
    }

    return ToClient.newBuilder()
        .setSolution(builder.build())
        .build()
}