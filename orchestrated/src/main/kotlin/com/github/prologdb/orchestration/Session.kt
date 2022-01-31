package com.github.prologdb.orchestration

import com.github.prologdb.dbms.SystemCatalog

/**
 * The mutable context in which a connection to the server
 * can be.
 */
class Session(@Volatile var systemCatalog: SystemCatalog) {
    /**
     * the currently selected knowledge base
     */
    @Volatile
    var knowledgeBase: String? = null

    /**
     * the currently selected module
     */
    @Volatile
    var module: String? = null
}