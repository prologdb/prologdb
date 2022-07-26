package com.github.prologdb.dbms.builtin

import com.github.prologdb.dbms.DatabaseProofSearchContext
import com.github.prologdb.runtime.PrologInternalError
import com.github.prologdb.runtime.module.ModuleScopeProofSearchContext
import com.github.prologdb.runtime.stdlib.nativeRule
import com.github.prologdb.runtime.term.Atom

val BuiltinDatabaseCurrentModule1 = nativeRule("current_module", 1) { args, ctxt ->
    val moduleName = when(ctxt) {
        is ModuleScopeProofSearchContext -> ctxt.module.declaration.moduleName
        is DatabaseProofSearchContext -> ctxt.moduleName
        else -> throw PrologInternalError("Cannot determine current module.")
    }

    return@nativeRule args[0].unify(Atom(moduleName), ctxt.randomVariableScope)
}