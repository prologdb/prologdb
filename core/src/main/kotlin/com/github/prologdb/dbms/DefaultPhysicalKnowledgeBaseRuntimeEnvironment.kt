package com.github.prologdb.dbms

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.async.Principal
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.dbms.builtin.DatabaseStandardLibraryModuleLoader
import com.github.prologdb.dbms.builtin.PhysicalDynamicPredicate
import com.github.prologdb.parser.ParseException
import com.github.prologdb.parser.lexer.Lexer
import com.github.prologdb.parser.lexer.LineEndingNormalizer
import com.github.prologdb.parser.parser.ParseResult
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.source.SourceUnit
import com.github.prologdb.runtime.*
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.module.*
import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.PrologCallable
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.runtime.util.OperatorRegistry
import com.github.prologdb.util.OverrideModule
import java.util.*
import com.github.prologdb.runtime.proofsearch.ProofSearchContext as RuntimeProofSearchContext

internal class DefaultPhysicalKnowledgeBaseRuntimeEnvironment private constructor(
    override val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase,
    override val database: PrologDatabase,
    moduleLoader: ModuleLoader
) : DefaultPrologRuntimeEnvironment(moduleLoader), PhysicalKnowledgeBaseRuntimeEnvironment {
    constructor(
        knowledgeBaseCatalog: SystemCatalog.KnowledgeBase,
        database: PrologDatabase
    ) : this(
        knowledgeBaseCatalog,
        database,
        ModuleLoader(knowledgeBaseCatalog)
    )

    override val defaultModuleName: String? = knowledgeBaseCatalog.defaultModule

    override fun deriveProofSearchContextForModule(
        deriveFrom: RuntimeProofSearchContext,
        moduleName: String
    ): PhysicalDatabaseProofSearchContext {
        if (moduleName !in loadedModules) {
            throw ModuleNotLoadedException(moduleName)
        }

        return ProofSearchContext(
            knowledgeBaseCatalog,
            moduleName,
            this,
            moduleLookupTables.getValue(moduleName),
            deriveFrom.principal,
            deriveFrom.authorization,
            deriveFrom.randomVariableScope
        )
    }

    override fun newProofSearchContext(authorization: Authorization): PhysicalDatabaseProofSearchContext {
        val moduleName = knowledgeBaseCatalog.defaultModule ?: rootModule.name
        return ProofSearchContext(
            knowledgeBaseCatalog,
            moduleName,
            this,
            moduleLookupTables.getValue(moduleName),
            UUID.randomUUID(),
            authorization,
            RandomVariableScope()
        )
    }

    private class ModuleLoader(private val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase) : com.github.prologdb.runtime.module.ModuleLoader {
        override fun load(reference: ModuleReference): Module {
            val fromSource = loadSource(reference)

            val moduleCatalog = knowledgeBaseCatalog.modulesByName[reference.moduleName]
            if (moduleCatalog == null || moduleCatalog.predicates.isEmpty()) {
                return fromSource
            }

            return OverrideModule(
                fromSource,
                moduleCatalog.predicates.associate { predicateCatalog ->
                    ClauseIndicator.of(predicateCatalog) to PhysicalDynamicPredicate(predicateCatalog)
                }
            )
        }

        private fun loadSource(reference: ModuleReference): Module {
            if (reference.pathAlias != DATABASE_MODULE_PATH_ALIAS) {
                return DatabaseStandardLibraryModuleLoader.load(reference)
            }

            val module = knowledgeBaseCatalog.modulesByName[reference.moduleName]
                ?: throw ModuleNotFoundException(reference)

            val parseResult = parseModuleSource(reference, module.prologSource)
            ParseException.failOnError(parseResult.reportings, "Failed to parse stored source for module $reference")

            return parseResult.item
                ?: throw PrologInternalError("Failed to parse stored source for module $reference. Got no errors and no result.")
        }

        val rootModule: Module = object : Module {
            override val allDeclaredPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val exportedPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val imports: List<ModuleImport> = knowledgeBaseCatalog.modules.map { module ->
                ModuleImport.Selective(ModuleReference(DATABASE_MODULE_PATH_ALIAS, module.name), emptyMap())
            }
            override val localOperators = ISOOpsOperatorRegistry
            override val name = "\$root"
        }
    }

    companion object {
        const val DATABASE_MODULE_PATH_ALIAS = "db"
        private val parser = PrologParser()

        fun parseModuleSource(reference: ModuleReference, source: String): ParseResult<ASTModule> {
            val lexer = Lexer(
                SourceUnit("module ${reference.moduleName}"),
                LineEndingNormalizer(source.iterator())
            )
            return parser.parseSourceFile(lexer, DatabaseModuleSourceFileVisitor(reference.moduleName))
        }
    }

    private inner class ProofSearchContext(
        override val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase,
        override val moduleName: String,
        override val runtimeEnvironment: DefaultPhysicalKnowledgeBaseRuntimeEnvironment,
        private val lookupTable: Map<ClauseIndicator, Pair<ModuleReference, PrologCallable>>,
        override val principal: Principal,
        override val authorization: Authorization,
        override val randomVariableScope: RandomVariableScope
    ) : PhysicalDatabaseProofSearchContext {
        private val selfModule = runtimeEnvironment.getLoadedModule(moduleName)
        override val fulfillAttach: suspend LazySequenceBuilder<Unification>.(Query, initialVariables: VariableBucket) -> Unification? = { q, variables ->
            val executionPlan = runtimeEnvironment.database.executionPlanner.planExecution(q, this@ProofSearchContext, randomVariableScope)
            yieldAllFinal(
                executionPlan
                    .invoke(this@ProofSearchContext, LazySequence.of(Pair(variables, Unit)))
                    .mapRemaining { (variables, _) -> Unification(variables) }
            )
        }

        override val operators: OperatorRegistry = selfModule.localOperators

        override fun resolveCallable(simpleIndicator: ClauseIndicator): Pair<FullyQualifiedClauseIndicator, PrologCallable> {
            selfModule.allDeclaredPredicates[simpleIndicator]?.let { callable ->
                val fqIndicator = FullyQualifiedClauseIndicator(moduleName, simpleIndicator)
                return Pair(fqIndicator, callable)
            }

            lookupTable[simpleIndicator]?.let { (sourceModule, callable) ->
                val fqIndicator = FullyQualifiedClauseIndicator(sourceModule.moduleName, ClauseIndicator.of(callable))

                return Pair(fqIndicator, callable)
            }

            throw PredicateNotDefinedException(simpleIndicator, this.selfModule)
        }

        override fun resolveModuleScopedCallable(goal: Clause): Triple<FullyQualifiedClauseIndicator, PrologCallable, Array<out Term>>? {
            if (goal.functor != ":" || goal.arity != 2 || goal !is CompoundTerm) {
                return null
            }

            val moduleNameTerm = goal.arguments[0]
            val unscopedGoal = goal.arguments[1]

            if (moduleNameTerm !is Atom || unscopedGoal !is CompoundTerm) {
                return null
            }

            val simpleIndicator = ClauseIndicator.of(unscopedGoal)

            if (moduleNameTerm.name == this.moduleName) {
                val callable = selfModule.allDeclaredPredicates[simpleIndicator]
                    ?: throw PredicateNotDefinedException(simpleIndicator, selfModule)

                val fqIndicator = FullyQualifiedClauseIndicator(this.moduleName, simpleIndicator)
                return Triple(fqIndicator, callable, unscopedGoal.arguments)
            }

            val module = runtimeEnvironment.loadedModules[moduleNameTerm.name]
                ?: throw ModuleNotLoadedException(moduleNameTerm.name)

            val callable = module.exportedPredicates[simpleIndicator]
                ?: if (simpleIndicator in module.allDeclaredPredicates) {
                    throw PredicateNotExportedException(FullyQualifiedClauseIndicator(module.name, simpleIndicator), selfModule)
                } else {
                    throw PredicateNotDefinedException(simpleIndicator, module)
                }

            return Triple(
                FullyQualifiedClauseIndicator(module.name, simpleIndicator),
                callable,
                unscopedGoal.arguments
            )
        }

        override fun deriveForModuleContext(moduleName: String): PhysicalDatabaseProofSearchContext {
            return runtimeEnvironment.deriveProofSearchContextForModule(this, moduleName)
        }

        override fun toString() = "context of module $moduleName"
    }
}