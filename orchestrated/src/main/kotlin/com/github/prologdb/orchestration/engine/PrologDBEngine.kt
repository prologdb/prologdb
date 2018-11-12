package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.dbms.PrologDatabaseManager
import com.github.prologdb.execplan.planner.NoOptimizationExecutionPlanner
import com.github.prologdb.net.session.DatabaseEngine
import com.github.prologdb.orchestration.SessionContext
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.term.PrologString
import com.github.prologdb.runtime.unification.Unification
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

internal val log = LoggerFactory.getLogger("prologdb.engine")

class PrologDBEngine(
    dataDirectory: Path
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
            knowledgeBases[kbName] = DatabaseManagerKnowledgeBase(
                PrologDatabaseManager(
                    dirManager.directoryForKnowledgeBase(kbName),
                    NoOptimizationExecutionPlanner()
                )
            )
            log.info("Knowledge base $kbName initialized.")
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
        val indicator = ClauseIndicator.of(command)
        if (globalDirectives.supportsDirective(indicator)) {
            return globalDirectives.startDirective(session, command, totalLimit)
        }

        val kb = session.knowledgeBase?.second ?: return lazyError(PrologRuntimeException("No knowledge base selected."))
        return kb.startDirective(session, command, totalLimit)
    }

    fun close() {
        TODO()

        // TODO: wait for all data to be written to disk
    }

    private val globalDirectives = ProgramaticServerKnowledgeBase {
        directive("select_knowledge_base"/1) { ctxt, args ->
            val name = (args[0] as? PrologString)?.toKotlinString()
                ?: return@directive lazyError<Unification>(PrologRuntimeException("Argument 1 to select_knowledge_base/1 must be a string, got ${args[0].prologTypeName}"))

            val base = knowledgeBases[name]
                ?: return@directive lazyError<Unification>(PrologRuntimeException("Knowledge base $name does not exist."))

            ctxt.knowledgeBase = Pair(name, base)
            LazySequence.of(Unification.TRUE)
        }
    }
}