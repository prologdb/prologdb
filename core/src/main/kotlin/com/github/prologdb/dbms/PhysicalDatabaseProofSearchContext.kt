package com.github.prologdb.dbms

import com.github.prologdb.runtime.proofsearch.Authorization

interface PhysicalDatabaseProofSearchContext : DatabaseProofSearchContext {
    val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase

    override fun deriveForModuleContext(moduleName: String, restrictAuthorization: Authorization): PhysicalDatabaseProofSearchContext
}