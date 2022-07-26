package com.github.prologdb.dbms

import com.github.prologdb.dbms.builtin.meta.global.*
import com.github.prologdb.parser.parser.DefaultModuleSourceFileVisitor
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.parser.SourceFileVisitor
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
import com.github.prologdb.runtime.stdlib.loader.StandardLibraryModuleLoader
import com.github.prologdb.runtime.proofsearch.ProofSearchContext as RuntimeProofSearchContext

class GlobalMetaKnowledgeBaseRuntimeEnvironment(override val database: PrologDatabase) : DefaultPrologRuntimeEnvironment(ModuleLoader), DatabaseRuntimeEnvironment {
    override val defaultModuleName = SCHEMA_MODULE_NAME

    init {
        // TODO: load root module
    }

    override fun newProofSearchContext(moduleName: String, authorization: Authorization): DatabaseProofSearchContext {
        val superContext = super.newProofSearchContext(moduleName, authorization)
        return if (superContext is ModuleScopeProofSearchContext) {
            ProofSearchContext(superContext.module.declaration.moduleName, superContext)
        } else {
            deriveProofSearchContextForModule(superContext, RootModule.declaration.moduleName)
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
        const val KNOWLEDGE_BASE_NAME = "\$meta"
        const val SCHEMA_MODULE_NAME = "schema"

        internal object RootModule : Module {
            override val allDeclaredPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val exportedPredicates: Map<ClauseIndicator, PrologCallable> = emptyMap()
            override val imports: List<ModuleImport> = listOf(
                ModuleImport.Full(ModuleReference(DefaultPhysicalKnowledgeBaseRuntimeEnvironment.DATABASE_MODULE_PATH_ALIAS, SCHEMA_MODULE_NAME))
            )
            override val localOperators = ISOOpsOperatorRegistry
            override val declaration = ModuleDeclaration("\$root")
        }

        object ModuleLoader : com.github.prologdb.runtime.module.ModuleLoader {
            private val PARSER = PrologParser()
            private val deletageLoader = CascadingModuleLoader(listOf(
                ClasspathPrologSourceModuleLoader(
                    sourceFileVisitorSupplier = this::getSourceFileVisitor,
                    classLoader = GlobalMetaKnowledgeBaseRuntimeEnvironment::class.java.classLoader,
                    parser = PARSER,
                    moduleReferenceToClasspathPath = { moduleRef ->
                        if (moduleRef.pathAlias == DefaultPhysicalKnowledgeBaseRuntimeEnvironment.DATABASE_MODULE_PATH_ALIAS && moduleRef.moduleName == SCHEMA_MODULE_NAME) {
                            "com/github/prologdb/dbms/meta_schema.pl"
                        } else {
                            throw ModuleNotFoundException(moduleRef)
                        }
                    }
                ),
                StandardLibraryModuleLoader
            ))

            override fun initiateLoading(reference: ModuleReference, runtime: PrologRuntimeEnvironment): com.github.prologdb.runtime.module.ModuleLoader.PrimedStage = deletageLoader.initiateLoading(reference, runtime)

            private fun getSourceFileVisitor(moduleReference: ModuleReference, runtime: PrologRuntimeEnvironment): SourceFileVisitor<Module> {
                val nativeImplementations = nativeImplementationsByModuleRef[moduleReference.toString()] ?: emptyMap()
                return NativeCodeSourceFileVisitorDecorator(
                    DefaultModuleSourceFileVisitor(runtime),
                    nativeImplementations,
                    PARSER
                )
            }

            private val nativeImplementationsByModuleRef: Map<String, Map<ClauseIndicator, NativeCodeRule>> = mapOf(
                "${DefaultPhysicalKnowledgeBaseRuntimeEnvironment.DATABASE_MODULE_PATH_ALIAS}($SCHEMA_MODULE_NAME)" to listOf(
                    BuiltinCreateKnowledgeBase1,
                    BuiltinDropKnowledgeBase1,
                    BuiltinKnowledgeBase1,
                    BuiltinCreateModule2,
                    BuiltinDropModule2,
                    BuiltinModule2
                )
            ).mapValues { (_, nativeCodes) ->
                nativeCodes.associateBy(ClauseIndicator.Companion::of)
            }
        }
    }

    private inner class ProofSearchContext(
        override val moduleName: String,
        private val delegate: RuntimeProofSearchContext
    ) : RuntimeProofSearchContext by delegate, DatabaseProofSearchContext {

        override val runtimeEnvironment = this@GlobalMetaKnowledgeBaseRuntimeEnvironment

        init {
            if (moduleName != SCHEMA_MODULE_NAME && moduleName != RootModule.declaration.moduleName) {
                throw ModuleNotLoadedException(moduleName)
            }
        }

        override fun deriveForModuleContext(moduleName: String): ProofSearchContext {
            if (this.moduleName == moduleName) {
                return this
            }

            return ProofSearchContext(moduleName, delegate.deriveForModuleContext(moduleName))
        }
    }
}