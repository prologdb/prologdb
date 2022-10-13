package com.github.prologdb.indexing

import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.FullyQualifiedClauseIndicator
import com.github.prologdb.runtime.query.AndQuery
import com.github.prologdb.runtime.query.OrQuery
import com.github.prologdb.runtime.query.PredicateInvocationQuery
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.Variable

/**
 * Defines data that should be indexed. Rows for the index are determined
 * by unifying one fact from the base predicate with [baseFactTemplate]
 * and then running [generator] (or just `true/0` alternatively) with the
 * instantiations. For every solution that results, the index entry can be
 * created with [IndexDefinition.keyVariables] and [IndexDefinition.storeAdditionally].
 */
class IndexTemplate(val moduleName: String, unscopedTemplate: Query) {
    val baseFactIndicator: FullyQualifiedClauseIndicator
    val baseFactTemplate: CompoundTerm
    val generator: Query?
    val variables: Set<Variable>

    init {
        when (unscopedTemplate) {
            is PredicateInvocationQuery -> {
                baseFactTemplate = unscopedTemplate.goal
                generator = null
            }
            is AndQuery -> {
                if (unscopedTemplate.goals.isEmpty()) {
                    throw InvalidIndexDefinitionException("Invalid template goal (empty conjunction)")
                }
                val baseFactQuery = unscopedTemplate.goals[0]
                if (baseFactQuery !is PredicateInvocationQuery) {
                    throw InvalidIndexDefinitionException("Index templates must start with a predicate invocation")
                }
                baseFactTemplate = baseFactQuery.goal
                generator = unscopedTemplate.goals
                    .drop(1)
                    .takeUnless { it.isEmpty() }
                    ?.let { generatorGoals ->
                        generatorGoals.singleOrNull() ?: AndQuery(generatorGoals.toTypedArray())
                    }
            }
            is OrQuery -> {
                throw InvalidIndexDefinitionException("Index templates must be based on conjunction")
            }
        }

        variables = baseFactTemplate.variables + (generator?.variables ?: emptySet())
        baseFactIndicator = FullyQualifiedClauseIndicator(moduleName, ClauseIndicator.of(baseFactTemplate))
    }

    val unscopedTemplate: Query by lazy {
        if (generator == null) {
            PredicateInvocationQuery(baseFactTemplate)
        } else {
            AndQuery(arrayOf(
                PredicateInvocationQuery(baseFactTemplate),
                generator
            ))
        }
    }
}