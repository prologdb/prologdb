package com.github.prologdb.orchestration

import com.github.prologdb.orchestration.engine.ServerKnowledgeBase

/**
 * The mutable context in which a connection to the server
 * can be.
 */
class SessionContext {
    /**
     * The currently selected knowledge-base.
     */
    var knowledgeBase: ServerKnowledgeBase? = null
}