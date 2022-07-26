package com.github.prologdb.dbms

import com.github.prologdb.runtime.PrologRuntimeEnvironment
import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.ProofSearchContext

interface DatabaseRuntimeEnvironment : PrologRuntimeEnvironment {
    val database: PrologDatabase
    val defaultModuleName: String?

    override fun newProofSearchContext(moduleName: String, authorization: Authorization): DatabaseProofSearchContext
    override fun deriveProofSearchContextForModule(deriveFrom: ProofSearchContext, moduleName: String): DatabaseProofSearchContext
}