package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.dbms.KnowledgeBase
import com.github.prologdb.orchestration.Session
import com.github.prologdb.runtime.proofsearch.ReadWriteAuthorization
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.unification.Unification

class PersistentKnowledgeBaseToServerAdapter(
    private val kb: KnowledgeBase
) : ServerKnowledgeBase {

    override val operators
        get() = kb.operators

    override val planningInformation = kb.planningInformation

    override fun startQuery(session: Session, query: Query, totalLimit: Long?): LazySequence<Unification> {
        return kb.fulfill(query, ReadWriteAuthorization)
    }

    override fun startDirective(session: Session, command: CompoundTerm, totalLimit: Long?): LazySequence<Unification> {
        return kb.invokeDirective(command.functor, ReadWriteAuthorization, command.arguments)
    }

    override fun close() {
        kb.close()
    }
}