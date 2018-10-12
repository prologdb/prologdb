package com.github.prologdb.net.session.message

data class QueryRelatedError(
    val kind: Kind,
    val shortMessage: String? = null,
    val additionalFields: Map<String, String> = emptyMap()
) {
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