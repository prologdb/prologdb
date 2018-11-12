package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.dbms.PrologDatabaseManager
import com.github.prologdb.net.session.DatabaseEngine
import com.github.prologdb.orchestration.SessionContext
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification

/**
 * Subset of [DatabaseEngine] used within [PrologDBEngine] to
 * denote a single knowledge base (either data-based or code-based)
 */
interface ServerKnowledgeBase {
    fun startQuery(session: SessionContext, query: Query, totalLimit: Long?): LazySequence<Unification>

    fun startDirective(session: SessionContext, command: Predicate, totalLimit: Long?): LazySequence<Unification>
}

class DatabaseManagerKnowledgeBase(
    private val mgr: PrologDatabaseManager
) : ServerKnowledgeBase {
    override fun startQuery(session: SessionContext, query: Query, totalLimit: Long?): LazySequence<Unification> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun startDirective(session: SessionContext, command: Predicate, totalLimit: Long?): LazySequence<Unification> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}