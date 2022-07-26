package com.github.prologdb.dbms.builtin.meta.global

import com.github.prologdb.ModuleAlreadyExistsException
import com.github.prologdb.dbms.KnowledgeBaseNotFoundException
import com.github.prologdb.dbms.SystemCatalog
import com.github.prologdb.dbms.builtin.nativeDatabaseRule
import com.github.prologdb.runtime.module.ModuleNotFoundException
import com.github.prologdb.runtime.module.ModuleNotLoadedException
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.unification.Unification

val BuiltinCreateModule2 = nativeDatabaseRule("create_module", 2) { args, ctxt ->
    val knowledgeBaseName = args.getTyped<Atom>(0).name
    val moduleName = args.getTyped<Atom>(1).name

    ctxt.runtimeEnvironment.database.dataDirectory.modifySystemCatalog { systemCatalog ->
        val knowledgeBaseCatalog = systemCatalog.knowledgeBases.firstOrNull { it.name == knowledgeBaseName }
            ?: throw KnowledgeBaseNotFoundException(knowledgeBaseName)

        if (moduleName in knowledgeBaseCatalog.modulesByName) {
            throw ModuleAlreadyExistsException(moduleName, "A module with name $moduleName already exists in knowledge base $knowledgeBaseName")
        }

        try {
            ctxt.runtimeEnvironment.getLoadedModule(moduleName)
            throw ModuleAlreadyExistsException(moduleName, "Module $moduleName in knowledge base $knowledgeBaseName is already defined as a non-physical module.")
        }
        catch (_: ModuleNotLoadedException) {}
        catch (_: ModuleNotFoundException) {}

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