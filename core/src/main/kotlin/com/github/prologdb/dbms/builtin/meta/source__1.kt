package com.github.prologdb.dbms.builtin.meta

import com.github.prologdb.dbms.DefaultPhysicalKnowledgeBaseRuntimeEnvironment
import com.github.prologdb.dbms.MetaKnowledgeBaseRuntimeEnvironment
import com.github.prologdb.dbms.builtin.nativeDatabaseRule
import com.github.prologdb.parser.ParseException
import com.github.prologdb.runtime.ArgumentTypeError
import com.github.prologdb.runtime.PrologInternalError
import com.github.prologdb.runtime.PrologInvocationContractViolationException
import com.github.prologdb.runtime.module.ModuleNotFoundException
import com.github.prologdb.runtime.module.ModuleReference
import com.github.prologdb.runtime.term.PrologString
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.Unification

val BuiltinSource1 = nativeDatabaseRule("source", 1) { args, ctxt ->
    val runtime = ctxt.runtimeEnvironment
    if (runtime !is MetaKnowledgeBaseRuntimeEnvironment) {
        throw PrologInvocationContractViolationException("${args.indicator} can only be invoked in a meta knowledge-base")
    }

    val moduleReference = ModuleReference(DefaultPhysicalKnowledgeBaseRuntimeEnvironment.DATABASE_MODULE_PATH_ALIAS, ctxt.moduleName)

    val arg = args[0]
    if (arg is Variable) {
        val moduleCatalog = runtime.knowledgeBaseCatalog.modulesByName[ctxt.moduleName] ?: throw ModuleNotFoundException(moduleReference)
        return@nativeDatabaseRule arg.unify(PrologString(moduleCatalog.prologSource), ctxt.randomVariableScope)
    }

    if (arg is PrologString) {
        ctxt.runtimeEnvironment.database.dataDirectory.modifySystemCatalog { systemCatalog ->
            val knowledgeBaseCatalog = systemCatalog.knowledgeBases.single { it.name == runtime.knowledgeBaseCatalog.name }
            val moduleCatalog = knowledgeBaseCatalog.modulesByName[ctxt.moduleName] ?: throw ModuleNotFoundException(moduleReference)

            val newSource = arg.toKotlinString()
            val parseResult = DefaultPhysicalKnowledgeBaseRuntimeEnvironment.parseModuleSource(moduleReference, newSource)
            ParseException.failOnError(parseResult.reportings)
            if (parseResult.item == null) {
                throw PrologInternalError("Failed to parse new source for module $moduleReference. Got no errors and no result.")
            }

            systemCatalog.copy(knowledgeBases = (systemCatalog.knowledgeBases - knowledgeBaseCatalog) + knowledgeBaseCatalog.copy(
                modules = (knowledgeBaseCatalog.modules - moduleCatalog) + moduleCatalog.copy(
                    prologSource = newSource
                )
            ))
        }

        return@nativeDatabaseRule Unification.TRUE
    }

    throw ArgumentTypeError(null, 0, arg, PrologString::class.java, Variable::class.java)
}