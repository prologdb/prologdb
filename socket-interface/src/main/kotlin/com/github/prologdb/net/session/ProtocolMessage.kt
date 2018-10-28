package com.github.prologdb.net.session

import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

annotation class ToServer
annotation class ToClient

/**
 * Message models, independent of the protocol.
 */
sealed class ProtocolMessage

@ToServer @ToClient
data class GeneralError(
    val message: String,
    val additionalInformation: Map<String, String> = emptyMap()
) : ProtocolMessage()

/**
 * Protocol-independent of the command from client to server
 * to initialize a new query.
 */
@ToServer
data class InitializeQueryCommand(
    val desiredQueryId: Int,
    val instruction: Query,
    val preInstantiations: VariableBucket? = null,
    val kind: Kind = Kind.QUERY,
    val totalLimit: Long?
) : ProtocolMessage() {
    enum class Kind {
        /**
         * The instruction is a query/modification against the
         * data in the database
         */
        QUERY,

        /**
         * The instruction is about the session-, knowledge
         * base- or server-configuration.
         */
        DIRECTIVE
    }
}

@ToClient
data class QueryOpenedMessage(
    val queryId: Int
) : ProtocolMessage()

@ToServer
data class ConsumeQuerySolutionsCommand(
    val queryId: Int,
    val amount: Int?,
    val closeAfterwards: Boolean,
    val handling: SolutionHandling
) : ProtocolMessage() {
    enum class SolutionHandling {
        RETURN,
        DISCARD
    }
}

@ToClient
data class QueryClosedMessage(
    val queryId: Int,
    val reason: CloseReason
) : ProtocolMessage() {
    enum class CloseReason {
        SOLUTIONS_DEPLETED,

        ABORTED_ON_USER_REQUEST,

        FAILED
    }
}

@ToClient
data class QueryRelatedError(
    val queryId: Int,
    val kind: Kind,
    val shortMessage: String? = null,
    val additionalFields: Map<String, String> = emptyMap()
) : ProtocolMessage() {
    enum class Kind {
        /* A term sent to the server did not follow its syntax (invalid binary prolog, incorrect prolog code)
         */
        INVALID_TERM_SYNTAX,

        /* A query initialization was received with a query_id that is currently still in use (not closed)
         */
        QUERY_ID_ALREADY_IN_USE,

        /* A QuerySolutionConsumption was received for a query_id that is currently not open.
         */
        QUERY_ID_NOT_IN_USE,

        /* A (not further specified) error occured while calculation a solution (e.g. argument not sufficiently instantiated)
         */
        ERROR_GENERIC,

        /* An assert or retract query attempted to modify a predicate which was not previously declared dynamic using the
         * dynamic/1 directive.
         */
        ERROR_PREDICATE_NOT_DYNAMIC,

        /* An assert or retract query was not executed because a constraint prevented it (e.g. unique constraint)
         */
        ERROR_CONSTRAINT_VIOLATION
    }
}

@ToClient
data class QuerySolutionMessage(
    val queryId: Int,

    val solution: Unification
) : ProtocolMessage()