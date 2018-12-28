package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.dbms.PersistentKnowledgeBase
import com.github.prologdb.orchestration.SessionContext
import com.github.prologdb.runtime.knowledge.ReadWriteAuthorization
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification

class PersistentKnowledgeBaseToServerAdapter(
    val adapted: PersistentKnowledgeBase
) : ServerKnowledgeBase {

    override val operators
        get() = adapted.operators

    override val planningInformation = adapted.planningInformation

    override fun startQuery(session: SessionContext, query: Query, totalLimit: Long?): LazySequence<Unification> {
        return adapted.fulfill(query, ReadWriteAuthorization)
    }

    override fun startDirective(session: SessionContext, command: Predicate, totalLimit: Long?): LazySequence<Unification> {
        return adapted.invokeDirective(command.name, ReadWriteAuthorization, command.arguments)
    }

    override fun close() {
        adapted.close()
    }
}