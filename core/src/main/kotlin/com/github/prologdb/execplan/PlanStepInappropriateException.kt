package com.github.prologdb.execplan

/**
 * Thrown when a [PlanStep] does not fit the schema (e.g. no appropriate index available)
 */
class PlanStepInappropriateException(message: String, cause: Throwable? = null) : PrologQueryException(message, cause)