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
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.source.SourceUnit
import com.github.prologdb.runtime.*
import com.github.prologdb.runtime.module.*
import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.PrologCallable
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.MathContext
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.runtime.util.OperatorRegistry
import com.github.prologdb.util.OverrideModule
import java.util.UUID
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

    init {
        knowledgeBaseCatalog.modules.forEach { catalogModule ->
            assureModuleLoaded(ModuleReference(DATABASE_MODULE_PATH_ALIAS, catalogModule.name))
        }
    }

    override fun deriveProofSearchContextForModule(
        deriveFrom: RuntimeProofSearchContext,
        moduleName: String
    ): PhysicalDatabaseProofSearchContext {
        getLoadedModule(moduleName)

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

    override fun newProofSearchContext(moduleName: String, authorization: Authorization): PhysicalDatabaseProofSearchContext {
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
        override fun initiateLoading(reference: ModuleReference, runtime: PrologRuntimeEnvironment): com.github.prologdb.runtime.module.ModuleLoader.PrimedStage {
            check(runtime is DatabaseRuntimeEnvironment)
            val primedStage = loadSource(reference, runtime)

            val moduleCatalog = knowledgeBaseCatalog.modulesByName[reference.moduleName]
            if (moduleCatalog == null || moduleCatalog.predicates.isEmpty()) {
                return primedStage
            }

            return object : com.github.prologdb.runtime.module.ModuleLoader.PrimedStage {
                override val declaration = primedStage.declaration
                override fun proceed(): com.github.prologdb.runtime.module.ModuleLoader.ParsedStage {
                    val parsedStage = primedStage.proceed()
                    return object : com.github.prologdb.runtime.module.ModuleLoader.ParsedStage {
                        override val module = OverrideModule(
                            parsedStage.module,
                            moduleCatalog.predicates.associate { predicateCatalog ->
                                ClauseIndicator.of(predicateCatalog) to PhysicalDynamicPredicate(predicateCatalog)
                            }
                        )
                    }
                }
            }
        }

        private fun loadSource(reference: ModuleReference, runtime: DatabaseRuntimeEnvironment): com.github.prologdb.runtime.module.ModuleLoader.PrimedStage {
            if (reference.pathAlias != DATABASE_MODULE_PATH_ALIAS) {
                return DatabaseStandardLibraryModuleLoader.initiateLoading(reference, runtime)
            }

            val module = knowledgeBaseCatalog.modulesByName[reference.moduleName]
                ?: throw ModuleNotFoundException(reference)

            val primedStage = parseModuleSource(reference, module.prologSource, runtime)
            return object : com.github.prologdb.runtime.module.ModuleLoader.PrimedStage {
                override val declaration = primedStage.declaration
                override fun proceed(): com.github.prologdb.runtime.module.ModuleLoader.ParsedStage {
                    val parsedStage = primedStage.proceed()
                    ParseException.failOnError(parsedStage.reportings, "Failed to parse stored source for module $reference")

                    return parsedStage
                }
            }
        }
    }

    companion object {
        const val DATABASE_MODULE_PATH_ALIAS = "db"
        private val parser = PrologParser()

        fun parseModuleSource(reference: ModuleReference, source: String, runtime: DatabaseRuntimeEnvironment): PrologParser.PrimedStage {
            val lexer = Lexer(
                SourceUnit("module ${reference.moduleName}"),
                LineEndingNormalizer(source.iterator())
            )
            return parser.parseSourceFile(lexer, DatabaseModuleSourceFileVisitor(runtime), ModuleDeclaration(reference.moduleName))
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
        override val module = runtimeEnvironment.getLoadedModule(moduleName)
        override val fulfillAttach: suspend LazySequenceBuilder<Unification>.(Query, initialVariables: VariableBucket) -> Unification? = { q, variables ->
            val executionPlan = runtimeEnvironment.database.executionPlanner.planExecution(q, this@ProofSearchContext, randomVariableScope)
            yieldAllFinal(
                executionPlan
                    .invoke(this@ProofSearchContext, LazySequence.of(Pair(variables, Unit)))
                    .mapRemaining { (variables, _) -> Unification(variables) }
            )
        }

        override val operators: OperatorRegistry = module.localOperators
        override val mathContext: MathContext = MathContext.DEFAULT

        override fun resolveCallable(simpleIndicator: ClauseIndicator): Pair<FullyQualifiedClauseIndicator, PrologCallable> {
            module.allDeclaredPredicates[simpleIndicator]?.let { callable ->
                val fqIndicator = FullyQualifiedClauseIndicator(moduleName, simpleIndicator)
                return Pair(fqIndicator, callable)
            }

            lookupTable[simpleIndicator]?.let { (sourceModule, callable) ->
                val fqIndicator = FullyQualifiedClauseIndicator(sourceModule.moduleName, ClauseIndicator.of(callable))

                return Pair(fqIndicator, callable)
            }

            throw PredicateNotDefinedException(simpleIndicator, this.module)
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
                val callable = module.allDeclaredPredicates[simpleIndicator]
                    ?: throw PredicateNotDefinedException(simpleIndicator, module)

                val fqIndicator = FullyQualifiedClauseIndicator(this.moduleName, simpleIndicator)
                return Triple(fqIndicator, callable, unscopedGoal.arguments)
            }

            val module = runtimeEnvironment.getLoadedModule(moduleNameTerm.name)

            val callable = module.exportedPredicates[simpleIndicator]
                ?: if (simpleIndicator in module.allDeclaredPredicates) {
                    throw PredicateNotExportedException(FullyQualifiedClauseIndicator(module.declaration.moduleName, simpleIndicator),
                        this.module
                    )
                } else {
                    throw PredicateNotDefinedException(simpleIndicator, module)
                }

            return Triple(
                FullyQualifiedClauseIndicator(module.declaration.moduleName, simpleIndicator),
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