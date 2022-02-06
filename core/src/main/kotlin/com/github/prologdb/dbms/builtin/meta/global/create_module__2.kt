package com.github.prologdb.dbms.builtin.meta.global

import com.github.prologdb.dbms.KnowledgeBaseNotFoundException
import com.github.prologdb.dbms.SystemCatalog
import com.github.prologdb.dbms.builtin.nativeDatabaseRule
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.unification.Unification

val BuiltinCreateModule2 = nativeDatabaseRule("create_module", 2) { args, ctxt ->
    val knowledgeBaseName = args.getTyped<Atom>(0).name
    val moduleName = args.getTyped<Atom>(1).name

    ctxt.database.dataDirectory.modifySystemCatalog { systemCatalog ->
        val knowledgeBaseCatalog = systemCatalog.knowledgeBases.firstOrNull { it.name == knowledgeBaseName }
            ?: throw KnowledgeBaseNotFoundException(knowledgeBaseName)

        if (moduleName in knowledgeBaseCatalog.modulesByName) {
            throw PrologRuntimeException("A module with name $moduleName already exists in knowledge base $knowledgeBaseName")
        }

        if (moduleName in ctxt.runtimeEnvironment.loadedModules) {
            throw PrologRuntimeException("Module $moduleName in knowledge base $knowledgeBaseName is already defined as a non-physical module.")
        }

        return@modifySystemCatalog systemCatalog.copy(knowledgeBases = (systemCatalog.knowledgeBases - knowledgeBaseCatalog) + knowledgeBaseCatalog.copy(
            modules = knowledgeBaseCatalog.modules + SystemCatalog.Module(
                moduleName,
                emptySet(),
                ""
            )
        ))
    }

    Unification.TRUE
}