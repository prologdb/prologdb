package com.github.prologdb.dbms.builtin

import com.github.prologdb.runtime.PrologInternalError
import com.github.prologdb.runtime.stdlib.nativeRule

val BuiltinDatabaseRetractAll1 = nativeRule("retractAll", 1) { args, ctxt ->
    throw PrologInternalError("${args.indicator} should not be invoked directly. Goals to this predicate should be replaced with appropriate execution plan functors by the execution planner.")
}