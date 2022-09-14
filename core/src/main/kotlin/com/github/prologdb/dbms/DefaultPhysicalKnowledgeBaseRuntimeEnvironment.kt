package com.github.prologdb.dbms

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.dbms.builtin.DatabaseStandardLibraryModuleLoader
import com.github.prologdb.dbms.builtin.PhysicalDynamicPredicate
import com.github.prologdb.parser.ParseException
import com.github.prologdb.parser.lexer.Lexer
import com.github.prologdb.parser.lexer.LineEndingNormalizer
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.source.SourceUnit
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.DefaultPrologRuntimeEnvironment
import com.github.prologdb.runtime.PrologRuntimeEnvironment
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.module.*
import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.ProofSearchContext
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.MathContext
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.util.OverrideModule
import java.util.UUID

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
        deriveFrom: ProofSearchContext,
        moduleName: String
    ): PhysicalDatabaseProofSearchContext {
        getLoadedModule(moduleName)

        if (deriveFrom is DatabaseProofSearchContextWrapper) {
            return deriveFrom.deriveForModuleContext(moduleName)
        }

        return DatabaseProofSearchContextWrapper(
           deriveFrom,
            knowledgeBaseCatalog,
            this,
        )
    }

    override fun newProofSearchContext(moduleName: String, authorization: Authorization): PhysicalDatabaseProofSearchContext {
        val module = getLoadedModule(moduleName)
        return DatabaseProofSearchContextWrapper(
            ModuleScopeProofSearchContext(
                module,
                this,
                moduleLookupTables.getValue(moduleName),
                UUID.randomUUID(),
                RandomVariableScope(),
                authorization,
                MathContext.DEFAULT,
            ),
            knowledgeBaseCatalog,
            this
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

    private inner class DatabaseProofSearchContextWrapper(
        val delegate: ProofSearchContext,
        override val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase,
        override val runtimeEnvironment: DefaultPhysicalKnowledgeBaseRuntimeEnvironment,
    ) : ProofSearchContext by delegate, PhysicalDatabaseProofSearchContext {
        override val fulfillAttach: suspend LazySequenceBuilder<Unification>.(Query, Unification) -> Unification? = { q, variables ->
            val executionPlan = runtimeEnvironment.database.executionPlanner.planExecution(q, this@DatabaseProofSearchContextWrapper, randomVariableScope)
            yieldAllFinal(
                executionPlan
                    .invoke(this@DatabaseProofSearchContextWrapper, LazySequence.of(Pair(variables, Unit)))
                    .mapRemaining { (variables, _) -> Unification(variables) }
            )
        }

        override fun deriveForModuleContext(moduleName: String): PhysicalDatabaseProofSearchContext {
            return DatabaseProofSearchContextWrapper(delegate.deriveForModuleContext(moduleName), knowledgeBaseCatalog, runtimeEnvironment)
        }
    }
}