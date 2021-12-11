package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.IrrelevantPrincipal
import com.github.prologdb.async.LazySequence

fun <T : Any> lazySequenceOfError(error: Throwable): LazySequence<T> = object : LazySequence<T> {
    override val principal = IrrelevantPrincipal
    override val state = LazySequence.State.FAILED

    init {
        error.fillInStackTrace()
    }

    override fun close() {
    }

    override fun step(): LazySequence.State = state

    override fun tryAdvance(): T? {
        throw error
    }
}