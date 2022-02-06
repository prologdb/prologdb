package com.github.prologdb.dbms

import com.github.prologdb.dbms.builtin.meta.global.BuiltinCreateKnowledgeBase1
import com.github.prologdb.dbms.builtin.meta.global.BuiltinDropKnowledgeBase1
import com.github.prologdb.dbms.builtin.meta.global.BuiltinKnowledgeBase1
import com.github.prologdb.parser.parser.DefaultModuleSourceFileVisitor
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.parser.SourceFileVisitor
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.DefaultPrologRuntimeEnvironment
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.module.CascadingModuleLoader
import com.github.prologdb.runtime.module.Module
import com.github.prologdb.runtime.module.ModuleImport
import com.github.prologdb.runtime.module.ModuleNotFoundException
import com.github.prologdb.runtime.module.ModuleReference
import com.github.prologdb.runtime.module.ModuleScopeProofSearchContext
import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.PrologCallable
import com.github.prologdb.runtime.stdlib.NativeCodeRule
import com.github.prologdb.runtime.stdlib.loader.ClasspathPrologSourceModuleLoader
import com.github.prologdb.runtime.stdlib.loader.NativeCodeSourceFileVisitorDecorator
import com.github.prologdb.runtime.stdlib.loader.StandardLibraryModuleLoader
import com.github.prologdb.runtime.proofsearch.ProofSearchContext as RuntimeProofSearchContext

class GlobalMetaKnowledgeBaseRuntimeEnvironment(override val database: PrologDatabase) : DefaultPrologRuntimeEnvironment(RootModule, ModuleLoader), DatabaseRuntimeEnvironment {
    override val defaultModuleName = SCHEMA_MODULE_NAME

    override fun newProofSearchContext(authorization: Authorization): DatabaseProofSearchContext {
        val superContext = super.newProofSearchContext(authorization)
        return if (superContext is ModuleScopeProofSearchContext) {
            ProofSearchContext(database, superContext.module.name, superContext)
        } else {
            deriveProofSearchContextForModule(superContext, rootModule.name)
        }
    }

    override fun deriveProofSearchContextForModule(
        deriveFrom: RuntimeProofSearchContext,
        moduleName: String
    ): DatabaseProofSearchContext {
        return if (deriveFrom is ProofSearchContext) {
            deriveFrom.deriveForModuleContext(moduleName)
        } else {
            ProofSearchContext(database, moduleName, super.deriveProofSearchContextForModule(deriveFrom, moduleName))
        }
    }

    companion object {
        const val KNOWLEDGE_BASE_NAME = "\$meta"
        const val SCHEMA_MODULE_NAME = "schema"

        internal object RootModule : Module {
            override val allDeclaredPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val exportedPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val imports: List<ModuleImport> = listOf(
                ModuleImport.Full(ModuleReference(PhysicalKnowledgeBaseRuntimeEnvironment.DATABASE_MODULE_PATH_ALIAS, SCHEMA_MODULE_NAME))
            )
            override val localOperators = ISOOpsOperatorRegistry
            override val name = "\$root"
        }

        object ModuleLoader : com.github.prologdb.runtime.module.ModuleLoader {
            private val PARSER = PrologParser()
            private val deletageLoader = CascadingModuleLoader(listOf(
                ClasspathPrologSourceModuleLoader(
                    sourceFileVisitorSupplier = { getSourceFileVisitor(it) },
                    classLoader = GlobalMetaKnowledgeBaseRuntimeEnvironment::class.java.classLoader,
                    parser = PARSER,
                    moduleReferenceToClasspathPath = { moduleRef ->
                        if (moduleRef.pathAlias == PhysicalKnowledgeBaseRuntimeEnvironment.DATABASE_MODULE_PATH_ALIAS && moduleRef.moduleName == SCHEMA_MODULE_NAME) {
                            "com/github/prologdb/dbms/meta_schema.pl"
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
                    PARSER
                )
            }

            private val nativeImplementationsByModuleRef: Map<String, Map<ClauseIndicator, NativeCodeRule>> = mapOf(
                "${PhysicalKnowledgeBaseRuntimeEnvironment.DATABASE_MODULE_PATH_ALIAS}($SCHEMA_MODULE_NAME)" to listOf(
                    BuiltinCreateKnowledgeBase1,
                    BuiltinDropKnowledgeBase1,
                    BuiltinKnowledgeBase1
                )
            ).mapValues { (_, nativeCodes) ->
                nativeCodes.associateBy(ClauseIndicator.Companion::of)
            }
        }
    }

    private class ProofSearchContext(
        override val database: PrologDatabase,
        override val moduleName: String,
        private val delegate: RuntimeProofSearchContext
    ) : RuntimeProofSearchContext by delegate, DatabaseProofSearchContext {

        init {
            if (moduleName != SCHEMA_MODULE_NAME && moduleName != RootModule.name) {
                throw PrologRuntimeException("Module $moduleName is not loaded.")
            }
        }

        override fun deriveForModuleContext(moduleName: String): ProofSearchContext {
            if (this.moduleName == moduleName) {
                return this
            }

            return ProofSearchContext(database, moduleName, delegate.deriveForModuleContext(moduleName))
        }
    }
}