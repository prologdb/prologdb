package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.orchestration.SessionContext
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.unification.Unification

/**
 * Allows for a smoother definition of clauses backed by kotlin code.
 */
open class ProgramaticServerKnowledgeBase(
    initCode: ProgramaticServerKnowledgeBaseBuilder.() -> Any?
) : ServerKnowledgeBase {

    private val directives: Map<ClauseIndicator, (SessionContext, Array<out Term>) -> LazySequence<Unification>>

    init {
        val builder = ProgramaticServerKnowledgeBaseBuilder()
        builder.initCode()
        this.directives = builder.directives
    }

    infix fun supportsDirective(indicator: ClauseIndicator): Boolean = indicator in directives

    override fun startQuery(session: SessionContext, query: Query, totalLimit: Long?): LazySequence<Unification> {
        return lazyError(PrologRuntimeException("Queries in programatic knowledge bases not supported yet."))
    }

    override fun startDirective(session: SessionContext, command: Predicate, totalLimit: Long?): LazySequence<Unification> {
        val indicator = ClauseIndicator.of(command)
        val code = directives[indicator] ?: return lazyError(PrologRuntimeException("Directive $indicator not defined."))

        val results = code(session, command.arguments)
        return if (totalLimit != null) results.limitRemaining(totalLimit) else results
    }
}

class ProgramaticServerKnowledgeBaseBuilder internal constructor() {

    internal val directives = mutableMapOf<ClauseIndicator, (SessionContext, Array<out Term>) -> LazySequence<Unification>>()

    operator fun String.div(arity: Int) = ClauseIndicator.of(this, arity)

    fun directive(indicator: ClauseIndicator, code: (SessionContext, Array<out Term>) -> LazySequence<Unification>) {
        if (indicator in directives) {
            throw IllegalStateException("Directive $this is already defined")
        }

        directives[indicator] = code
    }
}