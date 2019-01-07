package com.github.prologdb.orchestration

import com.github.prologdb.orchestration.engine.ServerKnowledgeBase

/**
 * The mutable context in which a connection to the server
 * can be.
 */
class SessionContext {
    /**
     * The currently selected knowledge-base. First: name,
     * second: the manager
     */
    var knowledgeBase: Pair<String, ServerKnowledgeBase>? = null
}