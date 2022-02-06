package com.github.prologdb.orchestration

import com.github.prologdb.dbms.SystemCatalog
import com.github.prologdb.runtime.PrologRuntimeEnvironment

/**
 * The mutable context in which a connection to the server
 * can be.
 */
class Session(@Volatile var systemCatalog: SystemCatalog) {
    @Volatile
    var runtimeEnvironment: PrologRuntimeEnvironment? = null

    @Volatile
    var module: String? = null
}