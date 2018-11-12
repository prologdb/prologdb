package com.github.prologdb.orchestration

import com.github.prologdb.dbms.PrologDatabaseManager

/**
 * The mutable context in which a connection to the server
 * can be.
 */
class SessionContext {
    /**
     * The currently selected knowledge-base. First: name,
     * second: the manager
     */
    var knowledgeBase: Pair<String, PrologDatabaseManager>? = null
}