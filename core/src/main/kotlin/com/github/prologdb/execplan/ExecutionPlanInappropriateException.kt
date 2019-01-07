package com.github.prologdb.execplan

/**
 * Thrown when a [PlanFunctor] does not fit the schema (e.g. no appropriate index available)
 */
class ExecutionPlanInappropriateException(message: String, cause: Throwable? = null) : PrologQueryException(message, cause)