package com.github.prologdb.execplan

import com.github.prologdb.runtime.knowledge.library.OperatorDefinition
import com.github.prologdb.runtime.knowledge.library.OperatorRegistry
import com.github.prologdb.runtime.knowledge.library.OperatorType

/**
 * Defines the operators that apply to the execution plan language / prolog-like language.
 */
object ExecutionPlanOperators : OperatorRegistry {
    private val operators = setOf(
        OperatorDefinition(1100, OperatorType.XFY, "|"),
        OperatorDefinition(1200, OperatorType.XFY, ";"),
        OperatorDefinition(400,  OperatorType.YFX, "/")
    )
    
    override val allOperators: Iterable<OperatorDefinition> = operators

    override fun getOperatorDefinitionsFor(name: String): Set<OperatorDefinition> {
        return operators.filter { it.name == name }.toSet()
    }
}