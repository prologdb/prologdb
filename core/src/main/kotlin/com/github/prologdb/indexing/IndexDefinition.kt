package com.github.prologdb.indexing

import com.github.prologdb.runtime.term.Variable

/**
 * Definition of an index, excluding features for implementation selection.
 */
data class IndexDefinition(
    /** Name of the index, unique per [ClauseIndicator] of the template fact */
    val name: String,
    val template: IndexTemplate,
    val keyVariables: Set<Variable>,
    val storeAdditionally: Set<Variable>
    // TODO sort directions??
) {
    init {
        keyVariables.firstOrNull { it !in template.variables }?.let {
            throw InvalidIndexDefinitionException("Key variable $it can never be instantiated by the index goal")
        }
        storeAdditionally.firstOrNull { it !in template.variables }?.let {
            throw InvalidIndexDefinitionException("Storage-only variable $it can never be instantiated by the index goal")
        }
    }
}