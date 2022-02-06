package com.github.prologdb.dbms

import com.github.prologdb.parser.ModuleDeclaration
import com.github.prologdb.parser.parser.DefaultModuleSourceFileVisitor
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.DefaultPrologRuntimeEnvironment
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.module.CascadingModuleLoader
import com.github.prologdb.runtime.module.Module
import com.github.prologdb.runtime.module.ModuleImport
import com.github.prologdb.runtime.module.ModuleNotFoundException
import com.github.prologdb.runtime.module.ModuleReference
import com.github.prologdb.runtime.proofsearch.PrologCallable
import com.github.prologdb.runtime.stdlib.NativeCodeRule
import com.github.prologdb.runtime.stdlib.loader.ClasspathPrologSourceModuleLoader
import com.github.prologdb.runtime.stdlib.loader.NativeCodeSourceFileVisitorDecorator
import com.github.prologdb.runtime.stdlib.loader.StandardLibraryModuleLoader

class MetaKnowledgeBaseRuntimeEnvironment(
    val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase
) : DefaultPrologRuntimeEnvironment(
    rootModuleFor(knowledgeBaseCatalog),
    META_MODULE_LOADER
) {
    companion object {
        private const val META_MODULE_PATH_ALIAS = "meta"
        private val META_MODULE_NATIVE_IMPLEMENTATIONS: Map<ClauseIndicator, NativeCodeRule> = mapOf()
        private val PARSER = PrologParser()

        private val META_MODULE_LOADER = CascadingModuleLoader(listOf(
            ClasspathPrologSourceModuleLoader(
                sourceFileVisitorSupplier = { metaModuleSourceFileVisitor(it.moduleName) },
                classLoader = MetaKnowledgeBaseRuntimeEnvironment::class.java.classLoader,
                parser = PARSER,
                moduleReferenceToClasspathPath = { moduleRef ->
                    if (moduleRef.pathAlias == META_MODULE_PATH_ALIAS) {
                        "com/github/prologdb/dbms/meta_of_module.pl"
                    } else {
                        throw ModuleNotFoundException(moduleRef)
                    }
                }
            ),
            StandardLibraryModuleLoader
        ))

        private fun metaModuleSourceFileVisitor(moduleName: String) = NativeCodeSourceFileVisitorDecorator(
            DefaultModuleSourceFileVisitor(
                ModuleDeclaration(moduleName)
            ),
            META_MODULE_NATIVE_IMPLEMENTATIONS,
            PARSER
        )

        private fun rootModuleFor(knowledgeBaseCatalog: SystemCatalog.KnowledgeBase): Module = object : Module {
            override val allDeclaredPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val exportedPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val imports: List<ModuleImport> = knowledgeBaseCatalog.modules.map { moduleCatalog ->
                ModuleImport.Selective(ModuleReference(META_MODULE_PATH_ALIAS, moduleCatalog.name), emptyMap())
            }
            override val localOperators = ISOOpsOperatorRegistry
            override val name = "\$root"
        }
    }
}