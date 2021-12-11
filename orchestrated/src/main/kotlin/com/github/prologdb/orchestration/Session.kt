package com.github.prologdb.orchestration

import com.github.prologdb.orchestration.engine.ServerKnowledgeBase
import com.github.prologdb.orchestration.engine.ServerModule

/**
 * The mutable context in which a connection to the server
 * can be.
 */
class Session {
    /**
     * The currently selected [ServerModule] and it's parent [ServerKnowledgeBase].
     */
    val module: Pair<ServerModule, ServerKnowledgeBase>? = null
}