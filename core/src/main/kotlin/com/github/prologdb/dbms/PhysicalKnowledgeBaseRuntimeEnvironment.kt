package com.github.prologdb.dbms

import com.github.prologdb.runtime.module.Module
import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.ProofSearchContext

interface PhysicalKnowledgeBaseRuntimeEnvironment : DatabaseRuntimeEnvironment {
    val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase
    override val database: PrologDatabase
    override val loadedModules: Map<String, Module>
    override fun deriveProofSearchContextForModule(
        deriveFrom: ProofSearchContext,
        moduleName: String
    ): PhysicalDatabaseProofSearchContext

    override fun newProofSearchContext(authorization: Authorization): PhysicalDatabaseProofSearchContext
}

