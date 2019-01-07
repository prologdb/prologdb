package com.github.prologdb.dbms.builtin

import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.PrologException
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.VariableMapping
import com.github.prologdb.runtime.builtin.NativeCodeRule
import com.github.prologdb.runtime.builtin.getInvocationStackFrame
import com.github.prologdb.runtime.knowledge.ProofSearchContext
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.*
import com.github.prologdb.runtime.query.PredicateInvocationQuery
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.PrologInteger
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

typealias PrologDBBuiltinImplementation = suspend LazySequenceBuilder<Unification>.(Array<out Term>, DBProofSearchContext) -> Unit

/**
 * Verifies that the receiver is compatible with [PersistentKnowledgeBase]. If so,
 * returns a correctly typed view.
 * @throws PrologException If the receiver cannot be loaded into a [PersistentKnowledgeBase]
 */
fun Library.asDBCompatible(): DBLibrary {
    val dynamicExports = this.dynamicExports
    if (dynamicExports.isNotEmpty()) {
        throw PrologException("Runtime Library $name is not compatible with ${DBLibrary::class.simpleName}: has dynamic exports")
    }

    val libName = this.name
    val operators = this.operators

    val exports = HashMap<ClauseIndicator, Rule>(exports.size)
    this.exports.flatMap { this.findFor(it) }
        .forEach { givenClause ->
            val indicator = ClauseIndicator.of(givenClause)
            if (givenClause is Rule) {
                exports[indicator] = givenClause
            }
            else if (givenClause is CompoundTerm) {
                exports[indicator] = givenClause.asRule()
            }
            else {
                throw PrologException("Runtime library $name is not compatible with ${DBLibrary::class.simpleName}: clause $indicator is neither a rule nor a fact.")
            }
        }

    return object : DBLibrary {
        override val name = libName
        override val exports = exports
        override val operators = operators
    }
}

/**
 * A library that can be loaded into a [PersistentKnowledgeBase].
 */
interface DBLibrary {
    val name: String
    val exports: Map<ClauseIndicator, Rule>
    val operators: OperatorRegistry
}

internal fun databaseBuiltin(name: String, arity: Int, code: PrologDBBuiltinImplementation): NativeCodeRule {
    return NativeCodeRule(
        name,
        arity,
        getInvocationStackFrame()
    ) { args, ctxt ->
        if (ctxt !is DBProofSearchContext) {
            throw PrologException("Cannot invoke prologdb builtin with ${ctxt::class.java.simpleName} (requires ${DBProofSearchContext::class.java.simpleName})")
        }

        code(args, ctxt)
    }
}


internal fun databaseCompatibleLibrary(name: String, definition: BuiltinLibraryBuilder.() -> Any?): DBLibrary {
    val exports = HashMap<ClauseIndicator, NativeCodeRule>()
    val operators = DefaultOperatorRegistry()

    definition(object : BuiltinLibraryBuilder {
        override fun add(builtin: NativeCodeRule) {
            exports[ClauseIndicator.of(builtin.head)] = builtin
        }

        override fun defineOperator(definition: OperatorDefinition) = operators.defineOperator(definition)
    })

    return object : DBLibrary {
        override val name = name
        override val exports = exports
        override val operators = operators
    }
}

internal interface BuiltinLibraryBuilder : OperatorRegistrationTarget {
    /**
     * Adds the given builtin to the library.
     */
    fun add(builtin: NativeCodeRule)
}

private object TrueQuery : PredicateInvocationQuery(CompoundTerm("=", arrayOf(PrologInteger(1), PrologInteger(1)))) {
    override fun substituteVariables(variableValues: VariableBucket) = this

    override fun withRandomVariables(randomVarsScope: RandomVariableScope, mapping: VariableMapping) = this

    override fun toString() = "true"
}

private fun CompoundTerm.asRule(): Rule = object : Rule(this, TrueQuery) {
    override val fulfill: suspend LazySequenceBuilder<Unification>.(CompoundTerm, ProofSearchContext) -> Unit = { invocation, ctxt ->
        val invocationMapping = VariableMapping()
        val randomInvocation = ctxt.randomVariableScope.withRandomVariables(invocation, invocationMapping)
        val randomHead = ctxt.randomVariableScope.withRandomVariables(head, VariableMapping())

        randomHead.unify(randomInvocation)?.let { unification ->
            val resolvedBucket = unification.variableValues.withVariablesResolvedFrom(invocationMapping)
            resolvedBucket.retainAll(invocation.variables)
            yield(Unification(resolvedBucket))
        }
    }
}