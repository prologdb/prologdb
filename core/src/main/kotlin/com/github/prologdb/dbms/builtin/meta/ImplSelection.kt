package com.github.prologdb.dbms.builtin.meta

import com.github.prologdb.runtime.ArgumentError
import com.github.prologdb.runtime.stdlib.TypedPredicateArguments
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.PrologList
import com.github.prologdb.runtime.term.PrologString
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.ImplFeature

sealed interface ImplSelection {
    data class ByFeatureSupport(val required: Set<ImplFeature> = emptySet(), val desired: Set<ImplFeature> = emptySet()) :
        ImplSelection
    data class Fixed(val id: String) : ImplSelection

    companion object {
        fun parse(args: TypedPredicateArguments, argIndex: Int): ImplSelection =
            args
            .getTyped<PrologList>(argIndex)
            .also {
                if (it.tail != null) {
                    throw ArgumentError(argIndex, "must not have a tail")
                }
            }
            .elements
            .fold<Term, ImplSelection?>(null) { acc, option ->
                if (acc is Fixed) {
                    throw ArgumentError(
                        argIndex,
                        "if a fixed implementation is requested, required&desired features cannot be specified."
                    )
                }

                if (option !is CompoundTerm) {
                    throw ArgumentError(3, "all options must be compounds")
                }

                val accOrDefault = acc as ByFeatureSupport? ?: ByFeatureSupport()
                when (option.functor) {
                    "require" -> {
                        if (option.arity != 1) {
                            throw ArgumentError(3, "the option require takes a single argument")
                        }
                        accOrDefault.copy(required = accOrDefault.required + ImplFeature(option.arguments[0]))
                    }
                    "desire" -> {
                        if (option.arity != 1) {
                            throw ArgumentError(3, "the option desire takes a single argument")
                        }
                        accOrDefault.copy(desired = accOrDefault.required + ImplFeature(option.arguments[0]))
                    }
                    "impl" -> {
                        if (option.arity != 1) {
                            throw ArgumentError(3, "the option impl takes a single argument")
                        }
                        val idTerm = option.arguments[0] as? PrologString
                            ?: throw ArgumentError(3, "the option impl requires a string argument")

                        if (acc == null) {
                            Fixed(idTerm.toKotlinString())
                        } else {
                            throw ArgumentError(
                                argIndex,
                                "if a fixed implementation is requested, required&desired features cannot be specified."
                            )
                        }
                    }
                    else -> throw ArgumentError(3, "Unsupported option: ${option.functor}")
                }
            }
            ?: ByFeatureSupport()
    }
}