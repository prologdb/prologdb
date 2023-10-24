package com.github.prologdb.dbms.builtin

import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.PrologRuntimeEnvironment
import com.github.prologdb.runtime.module.ModuleLoader
import com.github.prologdb.runtime.module.ModuleReference
import com.github.prologdb.runtime.proofsearch.PrologCallable

import com.github.prologdb.util.OverrideModule

object DatabaseStandardLibraryModuleLoader : ModuleLoader {
    private val stdlibLoader = ModuleLoader.discoverOnClasspath()

    override fun initiateLoading(reference: ModuleReference, runtime: PrologRuntimeEnvironment): ModuleLoader.PrimedStage {
        val fromDefaultStdlib = stdlibLoader.initiateLoading(reference, runtime)
        val overrides = overrides[reference] ?: return fromDefaultStdlib

        return object : ModuleLoader.PrimedStage {
            override val declaration = fromDefaultStdlib.declaration
            override fun proceed(): ModuleLoader.ParsedStage {
                val parsedStage = fromDefaultStdlib.proceed()
                return object : ModuleLoader.ParsedStage {
                    override val module = OverrideModule(parsedStage.module, overrides)
                }
            }
        }
    }

    private val overrides: Map<ModuleReference, Map<ClauseIndicator, PrologCallable>> = mapOf(
        ModuleReference("essential", "\$clauses") to listOf(
            BuiltinDatabaseAssert1,
            BuiltinDatabaseRetract1,
            BuiltinDatabaseRetractAll1,
        )
    )
        .map { (moduleRef, clauses) ->
            moduleRef to clauses.associateBy { ClauseIndicator.of(it) }
        }
        .toMap()
}