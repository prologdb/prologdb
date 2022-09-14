package com.github.prologdb.dbms.builtin.meta.global

import com.github.prologdb.ModuleAlreadyExistsException
import com.github.prologdb.dbms.KnowledgeBaseNotFoundException
import com.github.prologdb.dbms.builtin.nativeDatabaseRule
import com.github.prologdb.runtime.module.ModuleNotFoundException
import com.github.prologdb.runtime.term.Atom

val BuiltinDropModule2 = nativeDatabaseRule("drop_module", 2) { args, ctxt ->
    val knowledgeBaseName = args.getTyped<Atom>(0).name
    val moduleName = args.getTyped<Atom>(1).name

    ctxt.runtimeEnvironment.database.dataDirectory.modifySystemCatalog { systemCatalog ->
        val knowledgeBaseCatalog = systemCatalog.knowledgeBases.firstOrNull { it.name == knowledgeBaseName }
            ?: throw KnowledgeBaseNotFoundException(knowledgeBaseName)

        val moduleCatalog = knowledgeBaseCatalog.modulesByName[moduleName]
        if (moduleCatalog == null) {
            try {
                ctxt.runtimeEnvironment.getLoadedModule(moduleName)
                throw ModuleAlreadyExistsException(moduleName, "Module $moduleName in knowledge base $knowledgeBaseName is not a physical module, so it cannot be dropped.")
            }
            catch (ex: ModuleNotFoundException) {
                throw ModuleNotFoundException(ex.reference, "A module with name $moduleName does not exist in knowledge base $knowledgeBaseName", ex)
            }
        }

        return@modifySystemCatalog systemCatalog.copy(knowledgeBases = (systemCatalog.knowledgeBases - knowledgeBaseCatalog) + knowledgeBaseCatalog.copy(
            modules = knowledgeBaseCatalog.modules - moduleCatalog
        ))
    }

    Unification.TRUE
}