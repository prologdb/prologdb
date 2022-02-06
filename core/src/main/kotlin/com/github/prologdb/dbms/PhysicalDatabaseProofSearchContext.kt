package com.github.prologdb.dbms

interface PhysicalDatabaseProofSearchContext : DatabaseProofSearchContext {
    val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase

    override fun deriveForModuleContext(moduleName: String): PhysicalDatabaseProofSearchContext
}