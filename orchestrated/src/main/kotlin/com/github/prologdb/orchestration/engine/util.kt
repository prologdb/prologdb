package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.IrrelevantPrincipal
import com.github.prologdb.async.LazySequence
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.PrologList
import com.github.prologdb.runtime.term.Term
import kotlin.reflect.KClass

fun <T : Any> lazySequenceOfError(error: Throwable): LazySequence<T> = object : LazySequence<T> {
    override val principal = IrrelevantPrincipal
    override val state = LazySequence.State.FAILED

    init {
        //error.fillInStackTrace()
    }

    override fun close() {
    }

    override fun step(): LazySequence.State = state

    override fun tryAdvance(): T? {
        throw error
    }
}

/**
 * @receiver the given term must be an atom or a list of atoms. The atom names are matched
 * against the enum `T` case-insensitive.
 * @return The enum values of `T` that the given [Term] resembles.
 */
inline fun <reified T : Enum<T>> Term.asEnumValues(): List<T> = asEnumValuesOf(T::class, enumValues())

/**
 * @receiver the given term must be an atom or a list of atoms. The atom names are matched
 * against the enum `T` case-insensitive.
 * @return The enum values of `T` that the given [Term] resembles.
 */
fun <T : Enum<T>> Term.asEnumValuesOf(enumClass: KClass<T>, enumValues: Array<out T>): List<T> {
    val atomList = (this as? Atom) ?.let { listOf(it) }
        ?: ((this as? PrologList) ?.elements ?: emptyList()).map {
            it as? Atom ?: throw PrologRuntimeException("Cannot construct a list of ${enumClass.qualifiedName} from ${this@asEnumValuesOf}: list value $it is not an atom")
        }
        ?: throw PrologRuntimeException("Cannot construct a list of ${enumClass.qualifiedName} from a ${this@asEnumValuesOf.prologTypeName}: must a an atom or a list of atoms")
    
    return atomList.map { atom ->
        enumValues.find { enumValue -> enumValue.name.toLowerCase().equals(atom.name.toLowerCase()) }
            ?: throw PrologRuntimeException("Enumeration ${enumClass.qualifiedName} does not have an element that corresponds to ${atom.name}")
    }
}