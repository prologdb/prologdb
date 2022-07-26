package com.github.prologdb.dbms

import com.github.prologdb.dbms.builtin.DatabaseStandardLibraryModuleLoader
import com.github.prologdb.dbms.builtin.meta.BuiltinCreateDynamicPredicate2
import com.github.prologdb.dbms.builtin.meta.BuiltinDynamic2
import com.github.prologdb.dbms.builtin.meta.BuiltinSource1
import com.github.prologdb.parser.parser.DefaultModuleSourceFileVisitor
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.DefaultPrologRuntimeEnvironment
import com.github.prologdb.runtime.PrologRuntimeEnvironment
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.module.*
import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.PrologCallable
import com.github.prologdb.runtime.stdlib.NativeCodeRule
import com.github.prologdb.runtime.stdlib.loader.ClasspathPrologSourceModuleLoader
import com.github.prologdb.runtime.stdlib.loader.NativeCodeSourceFileVisitorDecorator
import com.github.prologdb.runtime.proofsearch.ProofSearchContext as RuntimeProofSearchContext

class MetaKnowledgeBaseRuntimeEnvironment(
    override val database: PrologDatabase,
    val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase
) : DatabaseRuntimeEnvironment, DefaultPrologRuntimeEnvironment(META_MODULE_LOADER) {
    override val defaultModuleName = ROOT_MODULE_NAME

    override fun newProofSearchContext(moduleName: String, authorization: Authorization): DatabaseProofSearchContext {
        val superContext = super.newProofSearchContext(moduleName, authorization)
        if (superContext is ModuleScopeProofSearchContext) {
            return ProofSearchContext(superContext.module.declaration.moduleName, superContext)
        } else {
            return deriveProofSearchContextForModule(superContext, ROOT_MODULE_NAME)
        }
    }

    override fun deriveProofSearchContextForModule(
        deriveFrom: RuntimeProofSearchContext,
        moduleName: String
    ): DatabaseProofSearchContext {
        return if (deriveFrom is ProofSearchContext) {
            deriveFrom.deriveForModuleContext(moduleName)
        } else {
            ProofSearchContext(moduleName, super.deriveProofSearchContextForModule(deriveFrom, moduleName))
        }
    }

    companion object {
        const val KNOWLEDGE_BASE_SPECIFIER_FUNCTOR = "meta"
        private const val META_MODULE_PATH_ALIAS = "meta"
        private const val ROOT_MODULE_NAME = "\$root"
        private const val COMMON_META_MODULE_NAME = "\$meta_module_common"
        private val META_MODULE_NATIVE_IMPLEMENTATIONS: Map<ClauseIndicator, NativeCodeRule> = listOf(
            BuiltinSource1,
            BuiltinCreateDynamicPredicate2,
            BuiltinDynamic2,
        ).associateBy(ClauseIndicator.Companion::of)
        private val PARSER = PrologParser()

        private val META_MODULE_LOADER = CascadingModuleLoader(listOf(
            ClasspathPrologSourceModuleLoader(
                sourceFileVisitorSupplier = { _, runtime -> metaModuleSourceFileVisitor(runtime) },
                classLoader = MetaKnowledgeBaseRuntimeEnvironment::class.java.classLoader,
                parser = PARSER,
                moduleReferenceToClasspathPath = { moduleRef ->
                    if (moduleRef.pathAlias == META_MODULE_PATH_ALIAS) {
                        "com/github/prologdb/dbms/meta_of_module.pl"
                    } else if (moduleRef.pathAlias == "essential" && moduleRef.moduleName == COMMON_META_MODULE_NAME) {
                        "com/github/prologdb/dbms/meta_of_module_common.pl"
                    } else {
                        throw ModuleNotFoundException(moduleRef)
                    }
                }
            ),
            DatabaseStandardLibraryModuleLoader
        ))

        private fun metaModuleSourceFileVisitor(runtime: PrologRuntimeEnvironment) = NativeCodeSourceFileVisitorDecorator(
            DefaultModuleSourceFileVisitor(runtime),
            META_MODULE_NATIVE_IMPLEMENTATIONS,
            PARSER
        )

        private fun rootModuleFor(knowledgeBaseCatalog: SystemCatalog.KnowledgeBase): Module = object : Module {
            override val allDeclaredPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val exportedPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val imports: List<ModuleImport> = DatabaseModuleSourceFileVisitor.DEFAULT_IMPORTS.toList() + listOf(
                ModuleImport.Full(ModuleReference("essential", COMMON_META_MODULE_NAME))
            ) + knowledgeBaseCatalog.modules.map { moduleCatalog ->
                ModuleImport.Selective(ModuleReference(META_MODULE_PATH_ALIAS, moduleCatalog.name), emptyMap(), emptySet())
            }
            override val localOperators = ISOOpsOperatorRegistry
            override val declaration = ModuleDeclaration(ROOT_MODULE_NAME)
        }
    }

    private inner class ProofSearchContext(
        override val moduleName: String,
        private val delegate: RuntimeProofSearchContext
    ) : RuntimeProofSearchContext by delegate, DatabaseProofSearchContext {
        override val runtimeEnvironment = this@MetaKnowledgeBaseRuntimeEnvironment
        override fun deriveForModuleContext(moduleName: String): DatabaseProofSearchContext {
            return ProofSearchContext(moduleName, delegate.deriveForModuleContext(moduleName))
        }
    }
}