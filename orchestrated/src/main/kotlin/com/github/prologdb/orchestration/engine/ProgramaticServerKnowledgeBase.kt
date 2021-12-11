package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.execplan.planner.PlanningInformation
import com.github.prologdb.orchestration.SessionContext
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.knowledge.library.OperatorRegistry
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.unification.Unification

/**
 * A knowledge base within which every clause is backed by kotlin code.
 */
open class ProgramaticServerKnowledgeBase(
    override val operators: OperatorRegistry = ISOOpsOperatorRegistry,
    initCode: ProgramaticServerKnowledgeBaseBuilder.() -> Any?
) : ServerKnowledgeBase {

    private val directives: Map<ClauseIndicator, (SessionContext, Array<out Term>) -> LazySequence<Unification>>

    init {
        val builder = ProgramaticServerKnowledgeBaseBuilder()
        builder.initCode()
        this.directives = builder.directives
    }

    infix fun supportsDirective(indicator: ClauseIndicator): Boolean = indicator in directives

    override fun startQuery(session: Session, query: Query, totalLimit: Long?): LazySequence<Unification> {
        return lazySequenceOfError(PrologRuntimeException("Queries in programatic knowledge bases not supported yet."))
    }

    override fun startDirective(session: SessionContext, command: CompoundTerm, totalLimit: Long?): LazySequence<Unification> {
        val indicator = ClauseIndicator.of(command)
        val code = directives[indicator] ?: return lazySequenceOfError(PrologRuntimeException("Directive $indicator not defined."))

        val results = code(session, command.arguments)
        return if (totalLimit != null) results.limitRemaining(totalLimit) else results
    }

    override fun close() {
        // Nothing to do
    }

    override val planningInformation = object : PlanningInformation {
        override val existingDynamicFacts: Set<ClauseIndicator> = emptySet()
        override val existingRules: Set<ClauseIndicator> = emptySet()
        override val staticBuiltins: Set<ClauseIndicator> = directives.keys
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