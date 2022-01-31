package com.github.prologdb.dbms

import com.github.prologdb.parser.lexer.Lexer
import com.github.prologdb.parser.lexer.LineEndingNormalizer
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.source.SourceUnit
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.DefaultPrologRuntimeEnvironment
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.module.Module
import com.github.prologdb.runtime.module.ModuleImport
import com.github.prologdb.runtime.module.ModuleNotFoundException
import com.github.prologdb.runtime.module.ModuleReference
import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.PrologCallable
import com.github.prologdb.runtime.proofsearch.ProofSearchContext
import java.util.UUID

class DatabaseRuntimeEnvironment private constructor(
    val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase,
    val database: PrologDatabase,
    moduleLoader: ModuleLoader
) : DefaultPrologRuntimeEnvironment(
    moduleLoader.rootModule,
    moduleLoader
) {
   constructor(
       knowledgeBaseCatalog: SystemCatalog.KnowledgeBase,
       database: PrologDatabase
   ): this(knowledgeBaseCatalog, database, ModuleLoader(knowledgeBaseCatalog))

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
        return DBProofSearchContextImpl(
            database,
            knowledgeBaseCatalog,
            rootModule.name,
            this,
            moduleLookupTables.getValue(rootModule.name),
            UUID.randomUUID(),
            authorization,
            RandomVariableScope()
        )
    }

    class ModuleLoader(val catalog: SystemCatalog.KnowledgeBase) : com.github.prologdb.runtime.module.ModuleLoader {
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

        val rootModule: Module = object : Module {
            override val allDeclaredPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val exportedPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val imports: List<ModuleImport> = catalog.modules.map { module ->
                ModuleImport.Selective(ModuleReference(DATABASE_MODULE_PATH_ALIAS, module.name), emptyMap())
            }
            override val localOperators = ISOOpsOperatorRegistry
            override val name = "\$root"
        }
    }

    companion object {
        const val DATABASE_MODULE_PATH_ALIAS = "db"
    }
}