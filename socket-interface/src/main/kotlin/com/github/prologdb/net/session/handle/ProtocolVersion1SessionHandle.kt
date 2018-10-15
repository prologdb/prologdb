package com.github.prologdb.net.session.handle

import com.github.prologdb.io.binaryprolog.BinaryPrologDeserializationException
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.io.util.ByteArrayOutputStream
import com.github.prologdb.net.NetworkProtocolException
import com.github.prologdb.net.PrologDeserializationException
import com.github.prologdb.net.PrologParseException
import com.github.prologdb.net.session.*
import com.github.prologdb.net.v1.messages.*
import com.github.prologdb.net.v1.messages.GeneralError
import com.github.prologdb.net.v1.messages.QueryRelatedError
import com.github.prologdb.net.v1.messages.ToServer
import com.github.prologdb.parser.lexer.Lexer
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.source.SourceUnit
import com.github.prologdb.runtime.knowledge.library.OperatorRegistry
import com.github.prologdb.runtime.term.Term
import com.google.protobuf.ByteString
import com.google.protobuf.GeneratedMessageV3
import java.io.DataOutputStream
import java.net.Socket
import java.util.*

internal class ProtocolVersion1SessionHandle(
    private val socket: Socket,
    private val termReader: ProtocolVersion1TermReader,
    private val termWriter: ProtocolVersion1TermWriter
) : SessionHandle {

    private val inputStream = socket.getInputStream()
    private val outputStream = socket.getOutputStream()

    private val outQueue: Queue<GeneratedMessageV3> = ArrayDeque()

    override fun popNextIncomingMessage(): ProtocolMessage {
        val versionMessage = ToServer.parseDelimitedFrom(inputStream)

        return when (versionMessage.commandCase!!) {
            ToServer.CommandCase.CONSUME_RESULTS -> versionMessage.consumeResults.toIndependent()
            ToServer.CommandCase.INIT_QUERY -> versionMessage.initQuery.toIndependent(termReader)
            ToServer.CommandCase.ERROR -> versionMessage.error.toIndependent()
            ToServer.CommandCase.COMMAND_NOT_SET -> throw NetworkProtocolException("command field of ToServer message not set")
        }
    }

    override fun queueMessage(message: ProtocolMessage) {
        if (message.javaClass.getAnnotation(com.github.prologdb.net.session.ToClient::class.java) == null) {
            throw IllegalArgumentException("Can only send messages intended for the client; see the ToClient annotation")
        }

        outQueue.add(message.toProtocol(termWriter))
    }

    override fun sendMessage(message: ProtocolMessage) {
        flushOutbox()
        internalSendDirectly(message.toProtocol(termWriter))
    }

    private fun internalSendDirectly(message: GeneratedMessageV3) {
        message.writeDelimitedTo(outputStream)
    }

    override fun flushOutbox(): Int {
        var nFlushed = 0
        outQueue.forEach {
            internalSendDirectly(it)
            nFlushed++
        }

        return nFlushed
    }

    override fun closeSession() {
        inputStream.close()
        outputStream.close()
        socket.close()
    }
}

internal class ProtocolVersion1TermReader(
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
                val result = parser.parseTerm(lexer, operatorRegistry, { it.hasNext() })
                if (result.isSuccess) {
                    result.item!!
                } else {
                    throw PrologParseException(result.reportings)
                }
            }
        }
    }
}

internal class ProtocolVersion1TermWriter(
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

private val SOURCE_UNIT_INSTRUCTION = SourceUnit("instruction")

private fun QueryInitialization.toIndependent(termReader: ProtocolVersion1TermReader) = InitializeQueryCommand(
    queryId,
    termReader.toRuntimeTerm(instruction, SOURCE_UNIT_INSTRUCTION),
    kind.toIndependent(),
    if (hasPrecalculateAmount()) precalculateAmount.toLong() else 0,
    if (hasLimit()) limit else null
)

private fun QueryInitialization.Kind.toIndependent() = when(this) {
    QueryInitialization.Kind.DIRECTIVE -> InitializeQueryCommand.Kind.DIRECTIVE
    QueryInitialization.Kind.QUERY -> InitializeQueryCommand.Kind.QUERY
}

private fun QuerySolutionConsumption.toIndependent() = ConsumeQuerySolutionsCommand(
    queryId,
    if (hasAmount()) amount else null,
    closeAfterwards,
    if (hasUpdatePrecalculateAmount()) (updatePrecalculateAmount.toLong() and 0xFFFFFFFF) else null,
    notifyAboutClose,
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

private fun ProtocolMessage.toProtocol(termWriter: ProtocolVersion1TermWriter): GeneratedMessageV3 = when(this) {
    is com.github.prologdb.net.session.GeneralError -> toProtocol()
    is QueryOpenedMessage -> toProtocol()
    is QueryClosedMessage -> toProtocol()
    is com.github.prologdb.net.session.QueryRelatedError -> toProtocol()
    is QuerySolutionMessage -> toProtocol(termWriter)
    else -> throw IllegalArgumentException("Cannot convert message of type ${this.javaClass.name} to protocol version 1 because that is not a to-client message")
}

private fun QueryOpenedMessage.toProtocol() = QueryOpenedEvent.newBuilder()
    .setQueryId(queryId)
    .build()

private fun QueryClosedMessage.CloseReason.toProtocol() = when(this) {
    QueryClosedMessage.CloseReason.SOLUTIONS_DEPLETED -> QueryClosedEvent.Reason.SOLUTIONS_DEPLETED
    QueryClosedMessage.CloseReason.ABORTED_ON_USER_REQUEST -> QueryClosedEvent.Reason.ABORTED_ON_USER_REQUEST
    QueryClosedMessage.CloseReason.FAILED -> QueryClosedEvent.Reason.FAILED
}

private fun QueryClosedMessage.toProtocol() = QueryClosedEvent.newBuilder()
    .setQueryId(queryId)
    .setReason(reason.toProtocol())
    .build()

private fun com.github.prologdb.net.session.GeneralError.toProtocol() = GeneralError.newBuilder()
    .setMessage(message)
    .putAllAdditionalInformation(additionalInformation)
    .build()

private fun com.github.prologdb.net.session.QueryRelatedError.toProtocol() = QueryRelatedError.newBuilder()
    .setQueryId(queryId)
    .setKind(kind.toProtocol())
    .setShortMessage(shortMessage)
    .putAllAdditionalInformation(additionalFields)
    .build()

private fun com.github.prologdb.net.session.QueryRelatedError.Kind.toProtocol() = when(this) {
    com.github.prologdb.net.session.QueryRelatedError.Kind.QUERY_ID_NOT_IN_USE -> QueryRelatedError.Kind.QUERY_ID_NOT_IN_USE
    com.github.prologdb.net.session.QueryRelatedError.Kind.ERROR_CONSTRAINT_VIOLATION -> QueryRelatedError.Kind.ERROR_CONSTRAINT_VIOLATION
    com.github.prologdb.net.session.QueryRelatedError.Kind.ERROR_GENERIC -> QueryRelatedError.Kind.ERROR_GENERIC
    com.github.prologdb.net.session.QueryRelatedError.Kind.ERROR_PREDICATE_NOT_DYNAMIC -> QueryRelatedError.Kind.ERROR_PREDICATE_NOT_DYNAMIC
    com.github.prologdb.net.session.QueryRelatedError.Kind.INVALID_TERM_SYNTAX -> QueryRelatedError.Kind.INVALID_TERM_SYNTAX
    com.github.prologdb.net.session.QueryRelatedError.Kind.QUERY_ID_ALREADY_IN_USE -> QueryRelatedError.Kind.QUERY_ID_ALREADY_IN_USE
}

private fun QuerySolutionMessage.toProtocol(termWriter: ProtocolVersion1TermWriter): QuerySolution {
    val builder = QuerySolution.newBuilder()
        .setQueryId(queryId)

    solution.variableValues.values.forEach {
        val term = it.second
        if (term != null) {
            builder.putInstantiations(it.first.name, termWriter.write(term))
        }
    }

    return builder.build()
}