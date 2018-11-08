package com.github.prologdb.indexing

import com.github.prologdb.runtime.knowledge.library.ClauseIndicator

/**
 * A description of indices available for a particular kind of predicate
 * (see [indicator])
 */
interface IndexByArgumentMap {
    val indicator: ClauseIndicator

    /**
     * The available [PredicateArgumentIndex]es available for the given argument index, by
     * type of the indexed argument.
     * @throws IllegalArgumentException if `argumentIndex < 0` or `argumentIndex >= indicator.arity`
     */
    operator fun get(argumentIndex: Int): IndexByTypeMap
}

class DefaultIndexByArgumentMap(override val indicator: ClauseIndicator) : IndexByArgumentMap {
    private val byTypeMaps = mutableMapOf<Int, IndexByTypeMap>()
    private val indexFactories = mutableSetOf<PredicateArgumentIndexFactory<out PredicateArgumentIndex>>()

    fun registerIndexFactory(factory: PredicateArgumentIndexFactory<out PredicateArgumentIndex>) {
        indexFactories.add(factory)
    }

    override fun get(argumentIndex: Int): IndexByTypeMap {
        if (argumentIndex < 0 || argumentIndex >= indicator.arity) {
            throw IllegalArgumentException("Argument index must be in [0; ${indicator.arity}) (0 and indicator arity)")
        }

        val map = byTypeMaps[argumentIndex]
        if (map != null) return map

        synchronized(byTypeMaps) {
            val otherThreadMap = byTypeMaps[argumentIndex]
            if (otherThreadMap == null) {
                val newMap = createNewByTypeMap(argumentIndex)
                byTypeMaps[argumentIndex] = newMap
                return newMap
            }

            return otherThreadMap
        }
    }

    private fun createNewByTypeMap(argumentIndex: Int): IndexByTypeMap {
        return DefaultIndeyByTypeMap(indicator, argumentIndex, indexFactories)
    }
}