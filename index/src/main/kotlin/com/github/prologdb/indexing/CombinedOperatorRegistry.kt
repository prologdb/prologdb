package com.github.prologdb.indexing

import com.github.prologdb.runtime.knowledge.library.DefaultOperatorRegistry
import com.github.prologdb.runtime.knowledge.library.MutableOperatorRegistry
import com.github.prologdb.runtime.knowledge.library.OperatorDefinition
import com.github.prologdb.runtime.knowledge.library.OperatorRegistry

/**
 * An [OperatorRegistry] that is split into an immutable part (= the ISO ops) and a mutable part (= user defined
 * and persisted).
 */
class CombinedOperatorRegistry : MutableOperatorRegistry {
    private val immutableRegistry: OperatorRegistry = DefaultOperatorRegistry(true)
    private val mutableRegistry: MutableOperatorRegistry = DefaultOperatorRegistry(false)

    override val allOperators: Iterable<OperatorDefinition> = object : Iterable<OperatorDefinition> {
        override fun iterator(): Iterator<OperatorDefinition> = (immutableRegistry.allOperators.asSequence() + mutableRegistry.allOperators.asSequence()).iterator()
    }

    override fun defineOperator(definition: OperatorDefinition) = mutableRegistry.defineOperator(definition)

    override fun getOperatorDefinitionsFor(name: String): Set<OperatorDefinition> {
        return immutableRegistry.getOperatorDefinitionsFor(name) + mutableRegistry.getOperatorDefinitionsFor(name)
    }
}