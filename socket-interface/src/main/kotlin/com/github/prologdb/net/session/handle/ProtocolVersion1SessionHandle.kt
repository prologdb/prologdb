package com.github.prologdb.net.session.handle

import com.github.prologdb.io.binaryprolog.BinaryPrologDeserializationException
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.io.util.ByteArrayOutputStream
import com.github.prologdb.net.*
import com.github.prologdb.net.session.*
import com.github.prologdb.net.v1.messages.*
import com.github.prologdb.net.v1.messages.GeneralError
import com.github.prologdb.net.v1.messages.QueryRelatedError
import com.github.prologdb.net.v1.messages.ToClient
import com.github.prologdb.net.v1.messages.ToServer
import com.github.prologdb.parser.lexer.Lexer
import com.github.prologdb.parser.lexer.Operator
import com.github.prologdb.parser.lexer.OperatorToken
import com.github.prologdb.parser.lexer.Token
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.sequence.TransactionalSequence
import com.github.prologdb.parser.source.SourceUnit
import com.github.prologdb.runtime.knowledge.library.OperatorRegistry
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Term
import com.google.protobuf.ByteString
import com.google.protobuf.GeneratedMessageV3
import io.reactivex.Observable
import java.io.DataOutputStream
import java.nio.channels.AsynchronousByteChannel

internal class ProtocolVersion1SessionHandle(
    private val channel: AsynchronousByteChannel,
    private val prologReader: ProtocolVersion1PrologReader,
    private val prologWriter: ProtocolVersion1PrologWriter
) : SessionHandle {

    override val incomingMessages: Observable<ProtocolMessage>

    init {
        val incomingVersionMessages = AsyncByteChannelDelimitedProtobufReader(ToServer::class.java, channel).observable

        incomingMessages = incomingVersionMessages
            .map { versionMessage ->
                when (versionMessage.commandCase!!) {
                    ToServer.CommandCase.CONSUME_RESULTS -> versionMessage.consumeResults.toIndependent()
                    ToServer.CommandCase.INIT_QUERY ->
                        try {
                            versionMessage.initQuery.toIndependent(prologReader)
                        }
                        catch (ex: Throwable) {
                            val kind: com.github.prologdb.net.session.QueryRelatedError.Kind
                            val additional = mutableMapOf<String, String>()

                            when(ex) {
                                is BinaryPrologDeserializationException -> {
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

                            throw QueryRelatedException(com.github.prologdb.net.session.QueryRelatedError(
                                versionMessage.initQuery.queryId,
                                kind,
                                ex.message,
                                additional
                            ))
                        }
                    ToServer.CommandCase.ERROR -> versionMessage.error.toIndependent()
                    ToServer.CommandCase.COMMAND_NOT_SET -> throw NetworkProtocolException("command field of ToServer message not set")
                }
            }
            .doOnError { ex ->
                if (ex is QueryRelatedException) {
                    ex.errorObject.toProtocol().writeDelimitedTo(channel)
                } else {
                    ToClient.newBuilder()
                        .setServerError(GeneralError.newBuilder()
                            .setMessage(ex.message)
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
        outQueue.close()
        channel.close()
    }
}

internal class ProtocolVersion1PrologReader(
    private val parser: PrologParser,
    private val binaryPrologReader: BinaryPrologReader,
    private val operatorRegistry: OperatorRegistry
) {
    fun toRuntimeTerm(protoTerm: com.github.prologdb.net.v1.messages.Term, source: SourceUnit): Term {
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
                val lexer = Lexer(source, protoTerm.data.toStringUtf8().iterator())
                val result = parser.parseTerm(lexer, operatorRegistry, PrologParser.STOP_AT_EOF)
                if (result.isSuccess) {
                    result.item!!
                } else {
                    throw PrologParseException(result)
                }
            }
        }
    }

    fun toRuntimeQuery(protoQuery: com.github.prologdb.net.v1.messages.Query, source: SourceUnit): Query {
        return when (protoQuery.type!!) {
            com.github.prologdb.net.v1.messages.Query.Type.BINARY -> {
                binaryPrologReader.readQueryFrom(protoQuery.data.asReadOnlyByteBuffer())
            }
            com.github.prologdb.net.v1.messages.Query.Type.STRING -> {
                val lexer = Lexer(source, protoQuery.data.toStringUtf8().iterator())
                val result = parser.parseQuery(lexer, operatorRegistry, STOP_AT_EOF_OR_FULL_STOP)
                if (result.isSuccess) {
                    result.item!!
                } else {
                    throw PrologParseException(result)
                }
            }
        }
    }

    private companion object {
        val STOP_AT_EOF_OR_FULL_STOP: (TransactionalSequence<Token>) -> Boolean = {
            if (!it.hasNext()) true else {
                it.mark()
                val next = it.next()
                it.rollback()
                next is OperatorToken && next.operator == Operator.FULL_STOP
            }
        }
    }
}

internal class ProtocolVersion1PrologWriter(
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

    fun write(query: Query): com.github.prologdb.net.v1.messages.Query {
        val (bufferStream, dataOut) = buffer.get()
        bufferStream.reset()
        binaryPrologWriter.writeQueryTo(query, dataOut)

        return com.github.prologdb.net.v1.messages.Query.newBuilder()
            .setType(com.github.prologdb.net.v1.messages.Query.Type.BINARY)
            .setData(ByteString.copyFrom(bufferStream.bufferOfData))
            .build()
    }
}

private val SOURCE_UNIT_INSTRUCTION = SourceUnit("instruction")

private fun QueryInitialization.toIndependent(prologReader: ProtocolVersion1PrologReader): InitializeQueryCommand {
    val cmd = InitializeQueryCommand(
        queryId,
        prologReader.toRuntimeQuery(instruction, SOURCE_UNIT_INSTRUCTION),
        kind.toIndependent(),
        if (hasLimit()) limit else null
    )

    return cmd
}
// TODO: prepared statement

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
        .setShortMessage(shortMessage)
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