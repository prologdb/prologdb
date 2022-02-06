package com.github.prologdb.dbms

import com.github.prologdb.runtime.proofsearch.ProofSearchContext
import com.github.prologdb.storage.fact.FactStore

interface DatabaseProofSearchContext : ProofSearchContext {
    val database: PrologDatabase
    val moduleName: String
    val runtimeEnvironment: DatabaseRuntimeEnvironment

    fun getFactStore(predicate: SystemCatalog.Predicate): FactStore {
        return database.getFactStore(predicate.uuid)
    }

    override fun deriveForModuleContext(moduleName: String): DatabaseProofSearchContext
}