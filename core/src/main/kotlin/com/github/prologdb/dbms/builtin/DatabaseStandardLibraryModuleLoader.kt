package com.github.prologdb.dbms.builtin

import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.module.Module
import com.github.prologdb.runtime.module.ModuleLoader
import com.github.prologdb.runtime.module.ModuleReference
import com.github.prologdb.runtime.proofsearch.PrologCallable
import com.github.prologdb.runtime.stdlib.loader.StandardLibraryModuleLoader
import com.github.prologdb.util.OverrideModule

object DatabaseStandardLibraryModuleLoader : ModuleLoader {
    override fun load(reference: ModuleReference): Module {
        val fromDefaultStdlib = StandardLibraryModuleLoader.load(reference)
        val overrides = overrides[reference] ?: return fromDefaultStdlib

        return OverrideModule(fromDefaultStdlib, overrides)
    }

    private val overrides: Map<ModuleReference, Map<ClauseIndicator, PrologCallable>> = mapOf(
        ModuleReference("essential", "\$dynamic") to listOf(
            BuiltinDatabaseCurrentModule1
        ).associateBy { ClauseIndicator.of(it) }
    )
}