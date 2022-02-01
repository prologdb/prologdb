package com.github.prologdb.dbms

import com.github.prologdb.dbms.builtin.meta.BuiltinCreateKnowledgeBase1
import com.github.prologdb.dbms.builtin.meta.BuiltinDropKnowledgeBase1
import com.github.prologdb.dbms.builtin.meta.BuiltinKnowledgeBase1
import com.github.prologdb.parser.lexer.Lexer
import com.github.prologdb.parser.lexer.LineEndingNormalizer
import com.github.prologdb.parser.parser.DefaultModuleSourceFileVisitor
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.parser.SourceFileVisitor
import com.github.prologdb.parser.source.SourceUnit
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.DefaultPrologRuntimeEnvironment
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.module.CascadingModuleLoader
import com.github.prologdb.runtime.module.Module
import com.github.prologdb.runtime.module.ModuleImport
import com.github.prologdb.runtime.module.ModuleLoader
import com.github.prologdb.runtime.module.ModuleNotFoundException
import com.github.prologdb.runtime.module.ModuleReference
import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.PrologCallable
import com.github.prologdb.runtime.proofsearch.ProofSearchContext
import com.github.prologdb.runtime.stdlib.NativeCodeRule
import com.github.prologdb.runtime.stdlib.loader.ClasspathPrologSourceModuleLoader
import com.github.prologdb.runtime.stdlib.loader.NativeCodeSourceFileVisitorDecorator
import com.github.prologdb.runtime.stdlib.loader.StandardLibraryModuleLoader
import java.util.UUID

interface DatabaseRuntimeEnvironment {
    val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase
    val database: PrologDatabase
    val loadedModules: Map<String, Module>
    fun deriveProofSearchContextForModule(
        deriveFrom: ProofSearchContext,
        moduleName: String
    ): DBProofSearchContext

    fun newProofSearchContext(authorization: Authorization): DBProofSearchContext
}

class PhysicalDatabaseRuntimeEnvironment private constructor(
    override val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase,
    override val database: PrologDatabase,
    moduleLoader: AssistModuleLoader
) : DefaultPrologRuntimeEnvironment(
    moduleLoader.rootModule,
    moduleLoader
), DatabaseRuntimeEnvironment {
   constructor(
       knowledgeBaseCatalog: SystemCatalog.KnowledgeBase,
       database: PrologDatabase
   ): this(
       knowledgeBaseCatalog,
       database,
       when(knowledgeBaseCatalog.name) {
           SystemCatalog.META_KNOWLEDGE_BASE_NAME -> MetaKnowledgeBaseModuleLoader
           else -> PhysicalModuleLoader(knowledgeBaseCatalog)
       }
   )

    override fun deriveProofSearchContextForModule(
        deriveFrom: ProofSearchContext,
        moduleName: String
    ): DBProofSearchContext {
        if (moduleName !in loadedModules) {
            throw PrologRuntimeException("Module $moduleName is not loaded.")
        }

        return DBProofSearchContextImpl(
            database,
            knowledgeBaseCatalog,
            moduleName,
            this,
            moduleLookupTables.getValue(moduleName),
            deriveFrom.principal,
            deriveFrom.authorization,
            deriveFrom.randomVariableScope
        )
    }

    override fun newProofSearchContext(authorization: Authorization): DBProofSearchContext {
        val moduleName = knowledgeBaseCatalog.defaultModule ?: rootModule.name
        return DBProofSearchContextImpl(
            database,
            knowledgeBaseCatalog,
            moduleName,
            this,
            moduleLookupTables.getValue(moduleName),
            UUID.randomUUID(),
            authorization,
            RandomVariableScope()
        )
    }

    private interface AssistModuleLoader : ModuleLoader {
        val rootModule: Module
    }

    // todo: make a special version of the standard library module loader with adapted essential($clauses)
    class PhysicalModuleLoader(val catalog: SystemCatalog.KnowledgeBase) : AssistModuleLoader {
        private val parser = PrologParser()
        override fun load(reference: ModuleReference): Module {
            if (reference.pathAlias != DATABASE_MODULE_PATH_ALIAS) {
                throw ModuleNotFoundException(reference)
            }

            val module = catalog.modulesByName[reference.moduleName]
                ?: throw ModuleNotFoundException(reference)

            val lexer = Lexer(SourceUnit("module ${reference.moduleName}"), LineEndingNormalizer(module.prologSource.iterator()))
            val result = parser.parseSourceFile(lexer, DatabaseModuleSourceFileVisitor(module.name))
            return result.item
                ?: throw IllegalStateException("Failed to parse stored source for module ${module.name}: " + result.reportings.first())
        }

        override val rootModule: Module = object : Module {
            override val allDeclaredPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val exportedPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val imports: List<ModuleImport> = catalog.modules.map { module ->
                ModuleImport.Selective(ModuleReference(DATABASE_MODULE_PATH_ALIAS, module.name), emptyMap())
            }
            override val localOperators = ISOOpsOperatorRegistry
            override val name = "\$root"
        }
    }

    object MetaKnowledgeBaseModuleLoader : AssistModuleLoader {
        private val _parser = PrologParser()
        private val classpathPrefix = "com/github/prologdb/dbms"

        private val deletageLoader = CascadingModuleLoader(listOf(
            ClasspathPrologSourceModuleLoader(
                sourceFileVisitorSupplier = { getSourceFileVisitor(it) },
                classLoader = MetaKnowledgeBaseModuleLoader::class.java.classLoader,
                parser = _parser,
                moduleReferenceToClasspathPath = { moduleRef ->
                    if (moduleRef.pathAlias == DATABASE_MODULE_PATH_ALIAS && moduleRef.moduleName == SystemCatalog.META_SCHEMA_MODULE_NAME) {
                        "$classpathPrefix/meta_schema.pl"
                    } else {
                        throw ModuleNotFoundException(moduleRef)
                    }
                }
            ),
            StandardLibraryModuleLoader
        ))

        override fun load(reference: ModuleReference): Module = deletageLoader.load(reference)

        private fun getSourceFileVisitor(moduleReference: ModuleReference): SourceFileVisitor<Module> {
            val nativeImplementations = nativeImplementationsByModuleRef[moduleReference.toString()] ?: emptyMap()
            return NativeCodeSourceFileVisitorDecorator(
                DefaultModuleSourceFileVisitor(),
                nativeImplementations,
                _parser
            )
        }

        private val nativeImplementationsByModuleRef: Map<String, Map<ClauseIndicator, NativeCodeRule>> = mapOf(
            "${DATABASE_MODULE_PATH_ALIAS}(${SystemCatalog.META_SCHEMA_MODULE_NAME})" to listOf(
                BuiltinCreateKnowledgeBase1,
                BuiltinDropKnowledgeBase1,
                BuiltinKnowledgeBase1
            )
        ).mapValues { (_, nativeCodes) ->
            nativeCodes.associateBy(ClauseIndicator.Companion::of)
        }

        override val rootModule: Module = object : Module {
            override val allDeclaredPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val exportedPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val imports: List<ModuleImport> = listOf(
                ModuleImport.Full(ModuleReference(DATABASE_MODULE_PATH_ALIAS, SystemCatalog.META_SCHEMA_MODULE_NAME))
            )
            override val localOperators = ISOOpsOperatorRegistry
            override val name = "\$root"
        }
    }

    companion object {
        const val DATABASE_MODULE_PATH_ALIAS = "db"
    }
}