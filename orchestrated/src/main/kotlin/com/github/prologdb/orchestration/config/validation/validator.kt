package com.github.prologdb.orchestration.config.validation

import com.fasterxml.jackson.annotation.JsonProperty
import javax.validation.ConstraintViolation
import javax.validation.ElementKind
import javax.validation.Path
import javax.validation.Validation
import kotlin.jvm.internal.ReflectionFactory
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KProperty
import kotlin.reflect.full.*

private val validator = Validation.buildDefaultValidatorFactory().validator

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

private val reflectionFactory = ReflectionFactory()

fun Path.toYAMLPath(rootBeanType: Class<*>): String = toYAMLPath(reflectionFactory.getOrCreateKotlinClass(rootBeanType))
inline fun <reified T : Any> Path.toYAMLPath(): String = toYAMLPath(T::class)
fun Path.toYAMLPath(rootBeanType: KClass<*>): String {

    val sb = StringBuilder("#${rootBeanType.simpleName}")
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
                sb.append(parameter.findAnnotation<JsonProperty>()?.value?.emptyToNull() ?: parameter.name)
                isFirst = false
            }
            ElementKind.PROPERTY -> {
                val property = contextType.propertyFor(node as Path.PropertyNode) !!

                if (!isFirst) sb.append('.')
                sb.append(property.findAnnotation<JsonProperty>()?.value?.emptyToNull() ?: property.name)
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
                sb.append(contextMethod.findAnnotation<JsonProperty>()?.value?.emptyToNull() ?: contextMethod.beanGetterPropertyName)

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
        TODO()
    }

/**
 * @return the receiver unless it is empty, in which case `null` is returned.
 */
private fun String.emptyToNull(): String? = if (isEmpty()) null else this