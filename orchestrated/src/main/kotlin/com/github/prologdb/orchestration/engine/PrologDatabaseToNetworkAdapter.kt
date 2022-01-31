package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.dbms.DataDirectoryManager
import com.github.prologdb.dbms.DatabaseRuntimeEnvironment
import com.github.prologdb.dbms.PrologDatabase
import com.github.prologdb.dbms.SystemCatalog
import com.github.prologdb.execplan.planner.ExecutionPlanner
import com.github.prologdb.net.session.DatabaseEngine
import com.github.prologdb.net.session.handle.STOP_AT_EOF_OR_FULL_STOP
import com.github.prologdb.orchestration.Session
import com.github.prologdb.parser.lexer.Lexer
import com.github.prologdb.parser.parser.ParseResult
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.parser.PrologParser.Companion.STOP_AT_EOF
import com.github.prologdb.parser.source.SourceUnit
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.PrologRuntimeEnvironment
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.ReadWriteAuthorization
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

class PrologDatabaseToNetworkAdapter(
    val database: PrologDatabase
) : DatabaseEngine<Session> {
    private val globalDirectives: Map<ClauseIndicator, (Session, Array<out Term>) -> LazySequence<Unification>> = mapOf(
        ClauseIndicator.of("knowledge_base", 1) to { session, args ->
            if (args[0] !is PrologString && args[0] !is Atom) {
                return@to lazySequenceOfError(PrologRuntimeException("Argument 1 to knowledge_base/1 must be a string or atom, got ${args[0].prologTypeName}"))
            }
            val name = (args[0] as? Atom)?.name ?: (args[0] as PrologString).toKotlinString()

            if (database.dataDirectory.systemCatalog.knowledgeBases.none { it.name == name }) {
                return@to lazySequenceOfError(PrologRuntimeException("Knowledge base $name does not exist."))
            }

            session.knowledgeBase = name
            LazySequence.of(Unification.TRUE)
        },
        ClauseIndicator.of("current_knowledge_base", 1) to { session, args ->
            val name = session.knowledgeBase?.let { PrologString(it) }

            LazySequence.ofNullable(name?.unify(args[0], RandomVariableScope()))
        },
        ClauseIndicator.of("create_knowledge_base", 1) to { session, args ->
            if (args[0] !is PrologString && args[0] !is Atom) {
                return@to lazySequenceOfError<Unification>(PrologRuntimeException("Argument 1 to create_knowledge_base/1 must be a string or atom, got ${args[0].prologTypeName}"))
            }
            val name = (args[0] as? Atom)?.name ?: (args[0] as PrologString).toKotlinString()
            database.createKnowledgeBase(name)
            LazySequence.of(Unification.TRUE)
        },
        ClauseIndicator.of("explain", 1) to { session, args ->
            val arg0 = args[0] as? CompoundTerm ?: return@to lazySequenceOfError<Unification>(PrologRuntimeException("Argument 0 to explain/1 must be a query."))

            val queryResult = parser.transformQuery(arg0)
            val query = queryResult.item
                ?: throw PrologRuntimeException("Failed to parse query: " + queryResult.reportings.first())

            val psc = database.getRuntimeEnvironment(session.systemCatalog, session.knowledgeBaseCatalog.name)
                .newProofSearchContext(ReadWriteAuthorization)
                .deriveForModuleContext(session.moduleCatalog.name)
            val plan = database.executionPlanner.planExecution(query, psc, RandomVariableScope())
            val solutionVars = VariableBucket()
            solutionVars.instantiate(Variable("Plan"), plan.explanation)
            LazySequence.of(Unification(solutionVars))
        }
    )

    override fun initializeSession() = Session(database.dataDirectory.systemCatalog)

    override fun onSessionDestroyed(state: Session) {
        // TODO?
    }

    override fun startQuery(session: Session, query: Query, totalLimit: Long?): LazySequence<Unification> {
        val psc = session.runtimeEnvironment.newProofSearchContext(ReadWriteAuthorization)
            .deriveForModuleContext(session.moduleCatalog.name)

        return buildLazySequence(psc.principal) {
            psc.fulfillAttach(this, query, VariableBucket())
        }
    }

    override fun startDirective(session: Session, command: CompoundTerm, totalLimit: Long?): LazySequence<Unification> {
        val indicator = ClauseIndicator.of(command)
        val directive = globalDirectives[indicator]
            ?: return lazySequenceOfError(PrologRuntimeException("Directive $indicator is not implemented."))

        return directive.invoke(session, command.arguments)
    }

    private val parser = PrologParser()

    override fun parseTerm(context: Session?, codeToParse: String, origin: SourceUnit): ParseResult<Term> {
        val operatorsForParsing = if (context?.knowledgeBase != null && context.module != null) {
            context.runtimeEnvironment.loadedModules.getValue(context.module!!).localOperators
        } else ISOOpsOperatorRegistry
        val lexer = Lexer(origin, codeToParse.iterator())
        return parser.parseTerm(lexer, operatorsForParsing, STOP_AT_EOF)
    }

    override fun parseQuery(context: Session?, codeToParse: String, origin: SourceUnit): ParseResult<Query> {
        val operatorsForParsing = if (context?.knowledgeBase != null && context.module != null) {
            context.runtimeEnvironment.loadedModules.getValue(context.module!!).localOperators
        } else ISOOpsOperatorRegistry
        val lexer = Lexer(origin, codeToParse.iterator())
        return parser.parseQuery(lexer, operatorsForParsing, STOP_AT_EOF_OR_FULL_STOP)
    }

    fun close() {
        database.close()
    }

    private val Session.knowledgeBaseCatalog: SystemCatalog.KnowledgeBase
        get() {
            val knowledgeBaseName = knowledgeBase ?: throw PrologRuntimeException(
                "No knowledge base selected. Select one with :- knowledge_base(name)."
            )

            val knowledgeBaseCatalog = systemCatalog.knowledgeBases
                .firstOrNull { it.name == knowledgeBaseName }
                ?: throw PrologRuntimeException("Internal error: selected knowledge base $knowledgeBaseName does not exist.")

            return knowledgeBaseCatalog
        }

    private val Session.moduleCatalog: SystemCatalog.Module
        get() {
            val moduleName = module ?: throw PrologRuntimeException(
                "No module selected. Select one with :- module(name)."
            )

            return knowledgeBaseCatalog.modulesByName[moduleName]
                ?: throw PrologRuntimeException("Internal error: selected module $moduleName does not exist in knowledge base $knowledgeBase")
        }

    private val Session.runtimeEnvironment: DatabaseRuntimeEnvironment
        get() = database.getRuntimeEnvironment(systemCatalog, knowledgeBaseCatalog.name)
}