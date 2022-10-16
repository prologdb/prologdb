package com.github.prologdb.indexing

import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.Unification
import java.util.*

/**
 * Definition of an index, excluding features for implementation selection.
 */
data class IndexDefinition(
    /** Name of the index, unique per [ClauseIndicator] of the template fact */
    val name: String,
    val template: IndexTemplate,
    val keyVariables: SortedSet<Variable>,
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

    val keyComparator: Comparator<Unification> = Comparator<Unification> { a, b ->
        val valuesA = keyVariables.map { a[it] }
        val valuesB = keyVariables.map { b[it] }

        valuesA
            .asSequence()
            .zip(valuesB.asSequence())
            .map { (valueA, valueB) -> valueA.compareTo(valueB) }
            .firstOrNull { it != 0 }
            ?: 0
    }

    fun getKey(entry: IndexEntry): IndexKey {
        return IndexKey(entry.data.subset(keyVariables))
    }
}