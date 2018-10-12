package com.github.prologdb.net.session.message

import com.github.prologdb.runtime.term.Term

/**
 * Protocol-independent of the command from client to server
 * to initialize a new query.
 */
data class InitializeQueryCommand(
    val desiredQueryId: Int,
    val instruction: Term,
    val kind: Kind = Kind.QUERY,
    val initialPrecalculationLimit: Long,
    val totalLimit: Long
) {
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