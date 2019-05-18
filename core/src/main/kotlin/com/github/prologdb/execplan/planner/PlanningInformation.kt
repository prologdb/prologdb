package com.github.prologdb.execplan.planner

import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.storage.fact.FactStore

/**
 * Exposes information to [ExecutionPlanner] for them to optimize
 * queries.
 */
interface PlanningInformation {
    /**
     * For these indicators there is already a [FactStore].
     */
    val existingDynamicFacts: Set<ClauseIndicator>

    /**
     * Rules are declared for these indicators
     */
    val existingRules: Set<ClauseIndicator>

    /**
     * These indicators are occupied by builtins.
     */
    val staticBuiltins: Set<ClauseIndicator>
}
