package com.github.prologdb.dbms

import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.ProofSearchContext

interface PhysicalKnowledgeBaseRuntimeEnvironment : DatabaseRuntimeEnvironment {
    val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase
    override val database: PrologDatabase
    override fun deriveProofSearchContextForModule(
        deriveFrom: ProofSearchContext,
        moduleName: String,
        restrictAuthorization: Authorization,
    ): PhysicalDatabaseProofSearchContext

    override fun newProofSearchContext(moduleName: String, authorization: Authorization): PhysicalDatabaseProofSearchContext
}

