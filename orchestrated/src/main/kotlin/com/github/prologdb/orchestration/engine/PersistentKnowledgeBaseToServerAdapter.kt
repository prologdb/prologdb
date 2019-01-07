package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.dbms.PersistentKnowledgeBase
import com.github.prologdb.orchestration.SessionContext
import com.github.prologdb.runtime.knowledge.ReadWriteAuthorization
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification

class PersistentKnowledgeBaseToServerAdapter(
    private val kb: PersistentKnowledgeBase
) : ServerKnowledgeBase {

    override val operators
        get() = kb.operators

    override val planningInformation = kb.planningInformation

    override fun startQuery(session: SessionContext, query: Query, totalLimit: Long?): LazySequence<Unification> {
        return kb.fulfill(query, ReadWriteAuthorization)
    }

    override fun startDirective(session: SessionContext, command: Predicate, totalLimit: Long?): LazySequence<Unification> {
        return kb.invokeDirective(command.name, ReadWriteAuthorization, command.arguments)
    }

    override fun close() {
        kb.close()
    }
}