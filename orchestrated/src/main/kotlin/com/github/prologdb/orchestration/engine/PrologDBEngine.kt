package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.net.session.DatabaseEngine
import com.github.prologdb.orchestration.SessionContext
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

internal val log = LoggerFactory.getLogger("prologdb.engine")

class PrologDBEngine(
    private val dataDirectory: Path
) : DatabaseEngine<SessionContext> {

    private val dirManager = ServerDataDirectoryManager(dataDirectory)

    /**
     * All the knowledge bases known to the engine
     */
    private val knowledgeBases: MutableMap<String, ServerKnowledgeBase> = ConcurrentHashMap()

    // discover knowledge bases
    init {
        val kbNames = dirManager.serverMetadata.load("knowledgeBases", Array<String>::class.java) ?: emptyArray()
        for (kbName in kbNames) {
            log.info("Initializing knowledge base $kbName")
            knowledgeBases[kbName] = TODO()
        }
    }

    override fun initializeSession() = SessionContext()

    override fun onSessionDestroyed(state: SessionContext) {
        // TODO?
    }

    override fun startQuery(session: SessionContext, query: Query, totalLimit: Long?): LazySequence<Unification> {
        val kb = session.knowledgeBase?.second ?: return lazyError(PrologRuntimeException("No knowledge base selected."))
        return kb.startQuery(session, query, totalLimit)
    }

    override fun startDirective(session: SessionContext, command: Predicate, totalLimit: Long?): LazySequence<Unification> {
        val kb = session.knowledgeBase?.second ?: return lazyError(PrologRuntimeException("No knowledge base selected."))
        return kb.startDirective(session, command, totalLimit)
    }

    fun close() {
        TODO()

        // TODO: wait for all data to be written to disk
        // TODO: write metadata
    }
}

