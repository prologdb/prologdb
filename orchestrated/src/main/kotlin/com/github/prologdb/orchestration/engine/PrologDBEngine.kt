package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.net.session.DatabaseEngine
import com.github.prologdb.orchestration.SessionContext
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import java.nio.file.Path

class PrologDBEngine(
    private val dataDirectory: Path
) : DatabaseEngine<SessionContext> {
    override fun initializeSession() = SessionContext()

    override fun onSessionDestroyed(state: SessionContext) {
        // TODO?
    }

    override fun startQuery(session: SessionContext, query: Query, totalLimit: Long?): LazySequence<Unification> {
        TODO()
    }

    override fun startDirective(session: SessionContext, command: Predicate, totalLimit: Long?): LazySequence<Unification> {
        TODO()
    }
}