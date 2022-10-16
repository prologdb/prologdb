package com.github.prologdb.dbms

import com.github.prologdb.indexing.FactIndex
import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.ProofSearchContext
import com.github.prologdb.storage.fact.FactStore

interface DatabaseProofSearchContext : ProofSearchContext {
    val runtimeEnvironment: DatabaseRuntimeEnvironment

    fun getFactStore(predicate: SystemCatalog.Predicate): FactStore {
        return runtimeEnvironment.database.getFactStore(predicate.uuid)
    }

    fun getFactIndex(predicate: SystemCatalog.Index): FactIndex {
        return runtimeEnvironment.database.getFactIndex(predicate.uuid)
    }

    override fun deriveForModuleContext(moduleName: String, restrictAuthorization: Authorization): DatabaseProofSearchContext
}