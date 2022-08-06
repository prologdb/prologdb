package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.dbms.GlobalMetaKnowledgeBaseRuntimeEnvironment
import com.github.prologdb.dbms.PhysicalKnowledgeBaseRuntimeEnvironment
import com.github.prologdb.dbms.PrologDatabase
import com.github.prologdb.dbms.SystemCatalog
import com.github.prologdb.net.session.DatabaseEngine
import com.github.prologdb.net.session.handle.STOP_AT_EOF_OR_FULL_STOP
import com.github.prologdb.orchestration.KnowledgeBaseNotSelectedException
import com.github.prologdb.orchestration.ModuleNotSelectedException
import com.github.prologdb.orchestration.Session
import com.github.prologdb.parser.ParseException
import com.github.prologdb.parser.lexer.Lexer
import com.github.prologdb.parser.parser.ParseResult
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.parser.PrologParser.Companion.STOP_AT_EOF
import com.github.prologdb.parser.source.SourceUnit
import com.github.prologdb.runtime.*
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.proofsearch.ReadWriteAuthorization
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.stdlib.TypedPredicateArguments
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket
import org.slf4j.LoggerFactory

internal val log = LoggerFactory.getLogger("prologdb.network-adapter")

class PrologDatabaseToNetworkAdapter(
    val database: PrologDatabase
) : DatabaseEngine<Session> {
    private val globalDirectives: Map<ClauseIndicator, (Session, TypedPredicateArguments) -> LazySequence<Unification>> = mapOf(
        ClauseIndicator.of("knowledge_base", 1) to { session, args ->
            // TODO: require ongoing TX to be finished
            // TODO: refresh system catalog here? because its also the TX boundary
            val specifier = args[0]
            val runtimeEnvironment = database.getRuntimeEnvironment(session.systemCatalog, specifier)

            session.runtimeEnvironment = runtimeEnvironment
            session.module = runtimeEnvironment.defaultModuleName

            LazySequence.of(Unification.TRUE)
        },
        ClauseIndicator.of("module", 1) to { session, args ->
            val moduleName = args.getTyped<Atom>(0).name
            val runtimeEnvironment = session.runtimeEnvironment ?: throw KnowledgeBaseNotSelectedException()
            val module = runtimeEnvironment.getLoadedModule(moduleName)

            session.module = module.declaration.moduleName

            LazySequence.of(Unification.TRUE)
        },
        ClauseIndicator.of("explain", 1) to { session, args ->
            val queryTerm = args.getTyped<CompoundTerm>(0)
            val queryResult = parser.transformQuery(queryTerm)
            ParseException.failOnError(queryResult.reportings)
            val query = queryResult.item
                ?: throw PrologInternalError("Failed to parse query. Got no errors and no result.")
            val runtimeEnvironment = session.runtimeEnvironment as? PhysicalKnowledgeBaseRuntimeEnvironment
                ?: throw PrologInvocationContractViolationException("In this context, execution plans are not used to execute queries. Cannot show a plan.")
            val moduleName = session.module ?: throw ModuleNotSelectedException()

            val psc = runtimeEnvironment
                .newProofSearchContext(moduleName, ReadWriteAuthorization)
                .deriveForModuleContext(session.moduleCatalog.name)
            val plan = database.executionPlanner.planExecution(query, psc, RandomVariableScope())
            val solutionVars = VariableBucket()
            solutionVars.instantiate(Variable("Plan"), plan.explanation)
            LazySequence.of(Unification(solutionVars))
        },
        ClauseIndicator.of("rename_knowledge_base", 2) to { session, args ->
            if (session.runtimeEnvironment !is GlobalMetaKnowledgeBaseRuntimeEnvironment) {
                throw PrologInvocationContractViolationException("Directive ${args.indicator} can only be invoked from the ${GlobalMetaKnowledgeBaseRuntimeEnvironment.KNOWLEDGE_BASE_NAME} knowledge base.")
            }

            val oldName = args.getTyped<Atom>(0).name
            val newName = args.getTyped<Atom>(1).name

            database.renameKnowledgeBase(oldName, newName)

            LazySequence.of(Unification.TRUE)
        }
    )

    override fun initializeSession() = Session(database.dataDirectory.systemCatalog)

    override fun onSessionDestroyed(state: Session) {
        // TODO?
    }

    override fun startQuery(session: Session, query: Query): LazySequence<Unification> {
        try {
            val runtimeEnvironment = session.runtimeEnvironment ?: throw KnowledgeBaseNotSelectedException()
            val moduleName = session.module ?: throw ModuleNotSelectedException()
            val psc = runtimeEnvironment.newProofSearchContext(moduleName, ReadWriteAuthorization)
                .deriveForModuleContext(session.module ?: throw ModuleNotSelectedException())

            return buildLazySequence(psc.principal) {
                psc.fulfillAttach(this, query, VariableBucket())
            }
        }
        catch (ex: PrologException) {
            return lazySequenceOfError(ex)
        }
    }

    override fun startDirective(session: Session, command: CompoundTerm): LazySequence<Unification> {
        val indicator = ClauseIndicator.of(command)
        val directive = globalDirectives[indicator]
            ?: return lazySequenceOfError(PrologUnsupportedOperationException("Directive $indicator is not implemented."))

        return try {
            directive.invoke(session, TypedPredicateArguments(indicator, command.arguments))
        }
        catch (ex: PrologException) {
            lazySequenceOfError(ex)
        }
    }

    private val parser = PrologParser()

    override fun parseTerm(context: Session?, codeToParse: String, origin: SourceUnit): ParseResult<Term> {
        val operatorsForParsing = context
            ?.runtimeEnvironment
            ?.let { runtime ->
                context.module?.let { runtime.getLoadedModule(it) }
            }
            ?.localOperators
            ?: ISOOpsOperatorRegistry

        val lexer = Lexer(origin, codeToParse.iterator())
        return parser.parseTerm(lexer, operatorsForParsing, STOP_AT_EOF)
    }

    override fun parseQuery(context: Session?, codeToParse: String, origin: SourceUnit): ParseResult<Query> {
        val operatorsForParsing = context
            ?.runtimeEnvironment
            ?.let { runtime ->
                context.module?.let { runtime.getLoadedModule(it) }
            }
            ?.localOperators
            ?: ISOOpsOperatorRegistry

        val lexer = Lexer(origin, codeToParse.iterator())
        return parser.parseQuery(lexer, operatorsForParsing, STOP_AT_EOF_OR_FULL_STOP)
    }

    fun close() {
        database.close()
    }

    private val Session.moduleCatalog: SystemCatalog.Module
        get() {
            val runtimeEnvironment = runtimeEnvironment ?: throw KnowledgeBaseNotSelectedException()
            val moduleName = module ?: throw ModuleNotSelectedException()
            if (runtimeEnvironment !is PhysicalKnowledgeBaseRuntimeEnvironment) {
                throw PrologInvocationContractViolationException("Cannot obtain a module catalog because the current context is not managed through the system catalog.")
            }

            return runtimeEnvironment.knowledgeBaseCatalog.modulesByName[moduleName]
                ?: throw PrologInternalError("Selected module $moduleName does not exist in knowledge base ${runtimeEnvironment.knowledgeBaseCatalog.name}")
        }
}