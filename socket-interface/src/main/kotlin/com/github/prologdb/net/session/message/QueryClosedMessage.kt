package com.github.prologdb.net.session.message

data class QueryClosedMessage(
    val queryId: Int,
    val reason: CloseReason
) {
    enum class CloseReason {
        SOLUTIONS_DEPLETED,

        ABORTED_ON_USER_REQUEST,

        FAILED
    }
}