package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.dbms.DataDirectoryManager
import com.github.prologdb.dbms.PersistentKnowledgeBase
import com.github.prologdb.dbms.builtin.asDBCompatible
import com.github.prologdb.execplan.planner.ExecutionPlanner
import com.github.prologdb.net.session.DatabaseEngine
import com.github.prologdb.net.session.handle.STOP_AT_EOF_OR_FULL_STOP
import com.github.prologdb.orchestration.SessionContext
import com.github.prologdb.parser.lexer.Lexer
import com.github.prologdb.parser.parser.ParseResult
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.parser.PrologParser.Companion.STOP_AT_EOF
import com.github.prologdb.parser.source.SourceUnit
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.builtin.ComparisonLibrary
import com.github.prologdb.runtime.builtin.EqualityLibrary
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.builtin.TypeSafetyLibrary
import com.github.prologdb.runtime.builtin.dict.DictLibrary
import com.github.prologdb.runtime.builtin.dynamic.DynamicsLibrary
import com.github.prologdb.runtime.builtin.dynamic.predicateToQuery
import com.github.prologdb.runtime.builtin.lists.ListsLibrary
import com.github.prologdb.runtime.builtin.math.MathLibrary
import com.github.prologdb.runtime.builtin.string.StringsLibrary
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.*
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket
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
            initKnowledgeBase(kbName)
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

    private val parser = PrologParser()

    override fun parseTerm(context: SessionContext?, codeToParse: String, origin: SourceUnit): ParseResult<Term> {
        val operatorsForParsing = context?.knowledgeBase?.second?.operators ?: ISOOpsOperatorRegistry
        val lexer = Lexer(origin, codeToParse.iterator())
        return parser.parseTerm(lexer, operatorsForParsing, STOP_AT_EOF)
    }

    override fun parseQuery(context: SessionContext?, codeToParse: String, origin: SourceUnit): ParseResult<Query> {
        val operatorsForParsing = context?.knowledgeBase?.second?.operators ?: ISOOpsOperatorRegistry
        val lexer = Lexer(origin, codeToParse.iterator())
        return parser.parseQuery(lexer, operatorsForParsing, STOP_AT_EOF_OR_FULL_STOP)
    }

    fun close() {
        log.info("Closing PrologDB engine")

        log.info("Taking all knowledge bases out of commission")
        val knowledgeBasesCopy = synchronized(knowledgeBaseCreateOrDropMutex) {
            val copy = HashMap(knowledgeBases)
            knowledgeBases.clear()
            copy
        }

        for ((kbName, kbInst) in knowledgeBasesCopy) {
            log.info("Closing knowledge base {} (waiting for all data to be written to disk, closing I/O handles, ...)", kbName)
            kbInst.close()
            log.info("Knowledge base {} closed", kbName)
        }

        dirManager.close()

        // TODO?
    }

    /**
     * Initializes the existing knowledge base with the given name. When this call returns
     * normally, the knowledge base has been added to [knowledgeBases].
     */
    private fun initKnowledgeBase(kbName: String) {
        log.info("Initializing knowledge base {}", kbName)

        synchronized(knowledgeBaseCreateOrDropMutex) {
            val directory = dirManager.directoryForKnowledgeBase(kbName).normalize()
            log.debug("Opening data directory for knowledge base {}: {}", kbName, directory)
            val kb = PersistentKnowledgeBase(
                DataDirectoryManager.open(directory),
                factStoreLoader,
                executionPlanner
            )

            log.debug("Loading default libraries into {}", PersistentKnowledgeBase::class.simpleName)
            // all prologdb knowledge-bases have the runtime pre-loaded
            kb.load(EqualityLibrary.asDBCompatible())
            kb.load(ComparisonLibrary.asDBCompatible())
            kb.load(MathLibrary.asDBCompatible())
            kb.load(ListsLibrary.asDBCompatible())
            kb.load(StringsLibrary.asDBCompatible())
            kb.load(TypeSafetyLibrary.asDBCompatible())
            kb.load(DynamicsLibrary.asDBCompatible())
            kb.load(DictLibrary.asDBCompatible())

            knowledgeBases[kbName] = PersistentKnowledgeBaseToServerAdapter(kb)
        }


        log.info("Knowledge base $kbName initialized.")
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
                initKnowledgeBase(name)

                return@directive LazySequence.of(Unification.TRUE)
            }
        }

        directive("explain"/1) { ctxt, args ->
            val arg0 = args[0] as? Predicate ?: return@directive lazyError<Unification>(PrologRuntimeException("Argument 0 to explain/1 must be a query."))

            val currentKB = ctxt.knowledgeBase?.second ?: return@directive lazyError<Unification>(PrologRuntimeException("No knowledge base selected."))

            val query = predicateToQuery(arg0)
            val plan = executionPlanner.planExecution(query, currentKB.planningInformation)
            val solutionVars = VariableBucket()
            solutionVars.instantiate(Variable("Plan"), plan.explanation)
            return@directive LazySequence.of(Unification(solutionVars))
        }
    }
}