package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.dbms.DataDirectoryManager
import com.github.prologdb.dbms.PersistentKnowledgeBase
import com.github.prologdb.execplan.planner.ExecutionPlanner
import com.github.prologdb.net.session.DatabaseEngine
import com.github.prologdb.orchestration.SessionContext
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.term.PrologString
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.storage.fact.FactStoreLoader
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

internal val log = LoggerFactory.getLogger("prologdb.engine")

class PrologDBEngine(
    dataDirectory: Path,
    private val executionPlanner: ExecutionPlanner,
    private val factStoreLoader: FactStoreLoader
) : DatabaseEngine<SessionContext> {

    private val dirManager = ServerDataDirectoryManager.open(dataDirectory)

    /**
     * All the knowledge bases known to the engine
     */
    private val knowledgeBases: MutableMap<String, ServerKnowledgeBase> = ConcurrentHashMap()
    private val knowledgeBaseCreateOrDropMutex = Any()

    // discover knowledge bases
    init {
        for (kbName in dirManager.serverMetadata.allKnowledgeBaseNames) {
            log.info("Initializing knowledge base $kbName")
            knowledgeBases[kbName] = DatabaseManagerKnowledgeBase(
                PersistentKnowledgeBase(
                    DataDirectoryManager.open(dirManager.directoryForKnowledgeBase(kbName)),
                    factStoreLoader,
                    executionPlanner
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
        // TODO: wait for all data to be written to disk

        dirManager.close()

        TODO()
    }

    private val globalDirectives = ProgramaticServerKnowledgeBase {
        directive("select_knowledge_base"/1) { ctxt, args ->
            if (args[0] !is PrologString && args[0] !is Atom) {
                return@directive lazyError<Unification>(PrologRuntimeException("Argument 1 to select_knowledge_base/1 must be a string or atom, got ${args[0].prologTypeName}"))
            }
            val name = (args[0] as? Atom)?.name ?: (args[0] as PrologString).toKotlinString()

            val base = knowledgeBases[name]
                ?: return@directive lazyError<Unification>(PrologRuntimeException("Knowledge base $name does not exist."))

            ctxt.knowledgeBase = Pair(name, base)
            LazySequence.of(Unification.TRUE)
        }

        directive("create_knowledge_base"/1) { ctxt, args ->
            if (args[0] !is PrologString && args[0] !is Atom) {
                return@directive lazyError<Unification>(PrologRuntimeException("Argument 1 to create_knowledge_base/1 must be a string or atom, got ${args[0].prologTypeName}"))
            }
            val name = (args[0] as? Atom)?.name ?: (args[0] as PrologString).toKotlinString()

            // TODO: check permission

            synchronized(knowledgeBaseCreateOrDropMutex) {
                val existing = knowledgeBases[name]
                if (existing != null) {
                    return@directive lazyError<Unification>(PrologRuntimeException("A knowledge base with name $name already exists."))
                }

                val directory = dirManager.directoryForKnowledgeBase(name)
                Files.createDirectories(directory)

                dirManager.serverMetadata.onKnowledgeBaseAdded(name)
                knowledgeBases[name] = DatabaseManagerKnowledgeBase(
                    PersistentKnowledgeBase(
                        DataDirectoryManager.open(directory),
                        factStoreLoader,
                        executionPlanner
                    )
                )

                return@directive LazySequence.of(Unification.TRUE)
            }
        }
    }
}