package com.github.prologdb.dbms

import com.github.prologdb.dbms.builtin.meta.global.*
import com.github.prologdb.parser.parser.DefaultModuleSourceFileVisitor
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.parser.SourceFileVisitor
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.DefaultPrologRuntimeEnvironment
import com.github.prologdb.runtime.PrologRuntimeEnvironment
import com.github.prologdb.runtime.module.*
import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.ProofSearchContext
import com.github.prologdb.runtime.stdlib.NativeCodeRule
import com.github.prologdb.runtime.stdlib.loader.ClasspathPrologSourceModuleLoader
import com.github.prologdb.runtime.stdlib.loader.NativeCodeSourceFileVisitorDecorator
import com.github.prologdb.runtime.stdlib.loader.StandardLibraryModuleLoader

class GlobalMetaKnowledgeBaseRuntimeEnvironment(override val database: PrologDatabase) : DefaultPrologRuntimeEnvironment(ModuleLoader), DatabaseRuntimeEnvironment {
    override val defaultModuleName = SCHEMA_MODULE_NAME

    init {
        assureModuleLoaded(ModuleReference(DefaultPhysicalKnowledgeBaseRuntimeEnvironment.DATABASE_MODULE_PATH_ALIAS, SCHEMA_MODULE_NAME))
    }

    override fun newProofSearchContext(moduleName: String, authorization: Authorization): DatabaseProofSearchContext {
        return DatabaseProofSearchContextWrapper(super.newProofSearchContext(moduleName, authorization))
    }

    override fun deriveProofSearchContextForModule(
        deriveFrom: ProofSearchContext,
        moduleName: String
    ): DatabaseProofSearchContext {
        return if (deriveFrom is DatabaseProofSearchContextWrapper) {
            deriveFrom.deriveForModuleContext(moduleName)
        } else {
            DatabaseProofSearchContextWrapper(super.deriveProofSearchContextForModule(deriveFrom, moduleName))
        }
    }

    companion object {
        const val KNOWLEDGE_BASE_NAME = "\$meta"
        const val SCHEMA_MODULE_NAME = "schema"

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

    private inner class DatabaseProofSearchContextWrapper(
        private val delegate: ProofSearchContext
    ) : ProofSearchContext by delegate, DatabaseProofSearchContext {

        override val runtimeEnvironment = this@GlobalMetaKnowledgeBaseRuntimeEnvironment

        init {
            if (delegate.module.declaration.moduleName != SCHEMA_MODULE_NAME) {
                throw ModuleNotLoadedException(delegate.module.declaration.moduleName)
            }
        }

        override fun deriveForModuleContext(moduleName: String): DatabaseProofSearchContextWrapper {
            if (delegate.module.declaration.moduleName == moduleName) {
                return this
            }

            return DatabaseProofSearchContextWrapper(delegate.deriveForModuleContext(moduleName))
        }
    }
}