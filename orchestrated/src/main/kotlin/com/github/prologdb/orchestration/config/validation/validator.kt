package com.github.prologdb.orchestration.config.validation

import javax.validation.ConstraintViolation
import javax.validation.ElementKind
import javax.validation.Path
import javax.validation.Validation

private val validator = Validation.buildDefaultValidatorFactory().validator

/**
 * @return `value`
 * @throws ValidationException If the given value violates any of its validation constraints.
 */
inline fun <reified T : Any> refuseInvalid(value: T): T = refuseInvalid(value, T::class.java)

/**
 * @return `value`
 * @throws ValidationException If the given value violates any of its validation constraints.
 */
fun <T : Any> refuseInvalid(value: T, type: Class<T>): T {
    val violations = validator.validate(value, type)
    if (violations.isEmpty()) return value

    throw ValidationException(value, violations)
}

class ValidationException(val offendingRoot: Any, val violations: Set<ConstraintViolation<*>>) : RuntimeException()

val Path.asYAMLPath: String
    get() {
        val pathB = StringBuilder()
        val contextType: Class<*>? = null
        for (node in this) {
            when (node.kind) {
                ElementKind.CONSTRUCTOR -> { /* nothing to do */ }
                ElementKind.METHOD -> {
                    val methodName = node.
                }
            }
        }
    }