package com.github.prologdb.dbms.builtin.meta.global

import com.github.prologdb.dbms.KnowledgeBaseNotFoundException
import com.github.prologdb.dbms.builtin.nativeDatabaseRule
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.unification.Unification

val BuiltinDropModule2 = nativeDatabaseRule("drop_module", 2) { args, ctxt ->
    val knowledgeBaseName = args.getTyped<Atom>(0).name
    val moduleName = args.getTyped<Atom>(1).name

    ctxt.database.dataDirectory.modifySystemCatalog { systemCatalog ->
        val knowledgeBaseCatalog = systemCatalog.knowledgeBases.firstOrNull { it.name == knowledgeBaseName }
            ?: throw KnowledgeBaseNotFoundException(knowledgeBaseName)

        if (moduleName in ctxt.runtimeEnvironment.loadedModules) {
            throw PrologRuntimeException("A module with name $moduleName does not exist in knowledge base $knowledgeBaseName")
        }

        val moduleCatalog = knowledgeBaseCatalog.modulesByName[moduleName]
            ?: throw PrologRuntimeException("Module $moduleName in knowledge base $knowledgeBaseName is not a physical module, so it cannot be dropped.")

        return@modifySystemCatalog systemCatalog.copy(knowledgeBases = (systemCatalog.knowledgeBases - knowledgeBaseCatalog) + knowledgeBaseCatalog.copy(
            modules = knowledgeBaseCatalog.modules - moduleCatalog
        ))
    }

    Unification.TRUE
}