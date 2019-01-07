package com.github.prologdb.orchestration.config.validation

import com.fasterxml.jackson.annotation.JsonProperty
import org.hibernate.validator.messageinterpolation.ResourceBundleMessageInterpolator
import org.hibernate.validator.resourceloading.PlatformResourceBundleLocator
import org.slf4j.Logger
import java.util.*
import javax.validation.*
import kotlin.jvm.internal.ReflectionFactory
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KProperty
import kotlin.reflect.full.*
import kotlin.reflect.jvm.jvmErasure

private val validator: Validator = {
    // force english validation messages
    val defaultLocator = PlatformResourceBundleLocator("org.hibernate.validator.ValidationMessages")
    val interpolator = ResourceBundleMessageInterpolator {
        defaultLocator.getResourceBundle(Locale.ENGLISH)
    }

    Validation.byDefaultProvider().configure()
        .messageInterpolator(interpolator)
        .buildValidatorFactory().validator
}()

/**
 * @return `value`
 * @throws ValidationException If the given value violates any of its validation constraints.
 */
fun <T : Any> refuseInvalid(value: T): T {
    val violations = validator.validate(value)
    if (violations.isEmpty()) return value

    throw ValidationException(value, violations)
}

class ValidationException(val offendingRoot: Any, val violations: Set<ConstraintViolation<*>>) : RuntimeException()

val ConstraintViolation<*>.prettyPrinted: String
    get() = "${propertyPath.toYAMLPath(rootBean::class)}: $message"

private fun Logger.logViolations(error: ValidationException, additionalMessage: String, to: (String, Throwable?) -> Any?) {
    var message = "$additionalMessage: ${error.violations.first().prettyPrinted}"
    if (error.violations.size > 1) {
        message += " (${error.violations.size - 1} additional violations)"
    }

    to(message, error.cause)
}

fun Logger.logViolationsError(error: ValidationException, additionalMessage: String) {
    if (isErrorEnabled) {
        logViolations(error, additionalMessage, this::error)
    }
}

private val reflectionFactory = ReflectionFactory()

fun Path.toYAMLPath(rootBeanType: Class<*>): String = toYAMLPath(reflectionFactory.getOrCreateKotlinClass(rootBeanType))
inline fun <reified T : Any> Path.toYAMLPath(): String = toYAMLPath(T::class)
fun Path.toYAMLPath(rootBeanType: KClass<*>): String {

    val sb = StringBuilder()
    var isFirst = true

    var contextType: KClass<*> = rootBeanType
    var contextMethod: KFunction<*>? = null

    for (node in this) {
        when (node.kind!!) {
            ElementKind.CONSTRUCTOR -> {
                // assume primary constructor
                contextMethod = contextType.primaryConstructor ?: contextType.constructors.first()
            }
            ElementKind.METHOD -> {
                contextMethod = contextType.methodFor(node as Path.MethodNode) !!
            }
            ElementKind.PARAMETER -> {
                // must be constructor
                node as Path.ParameterNode
                val parameter = contextMethod!!.valueParameters[node.parameterIndex]

                if (!isFirst) sb.append('.')
                sb.append(parameter.findAnnotation<JsonProperty>()?.value?.emptyToNull() ?: parameter.name!!.lowerCamelCaseToKebabCase())
                isFirst = false
            }
            ElementKind.PROPERTY -> {
                val property = contextType.propertyFor(node as Path.PropertyNode) !!

                contextType = property.returnType.jvmErasure

                if (!isFirst) sb.append('.')
                sb.append(property.findAnnotation<JsonProperty>()?.value?.emptyToNull() ?: property.name.lowerCamelCaseToKebabCase())
                isFirst = false
            }
            ElementKind.CONTAINER_ELEMENT -> {
                node as Path.ContainerElementNode
                if (Collection::class.java.isAssignableFrom(node.containerClass)) {

                    sb.append('[')
                    sb.append(node.key ?: node.index!!)
                    sb.append(']')
                    isFirst = false
                }
            }
            ElementKind.RETURN_VALUE -> {
                // getter
                contextMethod!!

                if (!isFirst) sb.append('.')
                sb.append(contextMethod.findAnnotation<JsonProperty>()?.value?.emptyToNull() ?: contextMethod.beanGetterPropertyName.lowerCamelCaseToKebabCase())

                isFirst = false
            }
            ElementKind.BEAN -> {
                /* cannot do anything, this is top-level, the node does not provide any useful context
                 */
            }
            ElementKind.CROSS_PARAMETER -> {
                /* cross-parameter errors relate to the parent object rather than the elements,
                 * so the textual representation is fine like it is
                 */
            }
        }
    }

    return sb.toString()
}

private fun KClass<*>.methodFor(mNode: Path.MethodNode): KFunction<*>? = memberFunctions
    .asSequence()
    .filter { it.name == mNode.name }
    .filter { it.valueParameters.size == mNode.parameterTypes.size }
    .filter {
        for (paramIndex in 0..it.valueParameters.size) {
            if (it.valueParameters[paramIndex].javaClass != mNode.parameterTypes[paramIndex]) {
                return@filter false
            }
        }

        return@filter true
    }
    .firstOrNull()

private fun KClass<*>.propertyFor(pNode: Path.PropertyNode): KProperty<*>? = memberProperties
    .asSequence()
    .filter { it.name == pNode.name }
    .firstOrNull()

private val KFunction<*>.beanGetterPropertyName: String
    get() {
        var name = name
        if (name.startsWith("get")) {
            name = name[3].toLowerCase() + name.substring(4)
        }

        return name
    }

/**
 * @return the receiver unless it is empty, in which case `null` is returned.
 */
private fun String.emptyToNull(): String? = if (isEmpty()) null else this

private fun String.lowerCamelCaseToKebabCase(): String {
    val parts = split(Regex("(?=[A-Z])"))
    return parts.joinToString(
        separator = "-",
        transform = String::toLowerCase
    )
}