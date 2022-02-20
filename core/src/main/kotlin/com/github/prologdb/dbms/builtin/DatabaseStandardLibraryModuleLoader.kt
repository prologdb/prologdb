package com.github.prologdb.dbms.builtin

import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.module.Module
import com.github.prologdb.runtime.module.ModuleLoader
import com.github.prologdb.runtime.module.ModuleReference
import com.github.prologdb.runtime.proofsearch.PrologCallable
import com.github.prologdb.runtime.stdlib.loader.StandardLibraryModuleLoader

object DatabaseStandardLibraryModuleLoader : ModuleLoader {
    override fun load(reference: ModuleReference): Module {
        val fromDefaultStdlib = StandardLibraryModuleLoader.load(reference)
        val overrides = overrides[reference] ?: return fromDefaultStdlib

        return object : Module {
            override val name = fromDefaultStdlib.name
            override val imports = fromDefaultStdlib.imports
            override val localOperators = fromDefaultStdlib.localOperators
            override val allDeclaredPredicates = HashMap(fromDefaultStdlib.allDeclaredPredicates).also { allDeclared ->
                overrides.forEach { (overrideIndicator, overrideImpl) ->
                    allDeclared[overrideIndicator] = overrideImpl
                }
            }
            override val exportedPredicates = fromDefaultStdlib.exportedPredicates.keys.associateWith(allDeclaredPredicates::getValue)
        }
    }

    private val overrides: Map<ModuleReference, Map<ClauseIndicator, PrologCallable>> = mapOf(
        ModuleReference("essential", "\$dynamic") to listOf(
            BuiltinDatabaseCurrentModule1
        ).associateBy { ClauseIndicator.of(it) }
    )
}