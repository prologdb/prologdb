package com.github.prologdb.orchestration.config

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.javaType
import kotlin.reflect.jvm.jvmErasure


/**
 * Given an instance of a data class: creates a deep clone including
 * the given overrides.
 * @receiver Assumes immutability
 */
infix fun <T : Any> T.plusOverrides(overrides: Map<ObjectPath<T>, Any>): T {
    assert(overrides.keys.map { it.rootType }.distinct().size == 1)

    return plusOverridesInternal(overrides as Map<ObjectPath<Any>, Any>)
}

private fun <T: Any> T.plusOverridesInternal(overrides: Map<ObjectPath<Any>, Any>): T {
    if (overrides.isEmpty()) return this

    val rootType = overrides.keys.first().rootType
    assert(rootType.isData)

    val onThisLevel = overrides
        .filter { it.key.size == 1 } // directly pointing at a property
        .map { Pair(it.key.firstNode.property.name, it.value) }
        .toMap()

    val onNextLevel = overrides.filter { it.key.size > 1 }

    val thisLevelCtor = rootType.primaryConstructor!!
    val invocationParams = Array<Any?>(thisLevelCtor.parameters.size) { index ->
        val ctorParam = thisLevelCtor.parameters[index]
        val property = rootType.memberProperties.first { it.name == ctorParam.name!! }

        val value = onThisLevel[ctorParam.name] ?: property.get(this)

        if (property.returnType.isLeaf) return@Array value

        val overridesForValue = onNextLevel
            .filter { it.key.firstNode.property == property }
            .map { Pair(it.key.subPath(1), it.value) }
            .toMap()

        return@Array value?.plusOverridesInternal(overridesForValue)
    }

    return thisLevelCtor.call(*invocationParams) as T
}

class ObjectPath<R : Any> private constructor(val rootType: KClass<R>, private val nodes: ArrayList<Node>) {

    constructor(rootType: KClass<R>, nodes: Iterable<ObjectPath.Node>): this(rootType, nodes.let {
        val list = ArrayList<Node>()
        it.forEach { node -> list.add(node) }
        list
    })

    val firstNode: Node = nodes.first()

    val size = nodes.size

    /**
     * The type of the property this path points to.
     */
    val target: KProperty<*> = nodes.last().property

    /**
     * @return a copy of this object path without the first `n` nodes.
     */
    fun subPath(n: Int): ObjectPath<Any> {
        assert(n >= 0)
        assert(n < nodes.size)

        if (n == 0) return this as ObjectPath<Any>

        val subList = nodes.subList(n, nodes.size)
        val contextType = firstNode.property.returnType.jvmErasure as KClass<Any>
        return ObjectPath(contextType, subList)
    }

    data class Node(val property: KProperty<*>)

    override fun toString() = "#${rootType.simpleName}.${nodes.map { it.property.name }.joinToString(".")}"

    companion object {
        inline fun <reified R: Any> parse(path: String): ObjectPath<R> = parse(R::class, path)
        fun <R : Any> parse(rootType: KClass<R>, path: String): ObjectPath<R> {
            // as long as only property nodes exist this really is straightforward

            val propertyNames = path.split('.')
            val nodes = LinkedList<Node>()

            var contextType = rootType as KClass<*>
            for (propertyName in propertyNames) {
                val property = contextType.memberProperties.firstOrNull { it.name == propertyName }
                if (property == null) {
                    val validSubPath = "#${rootType.simpleName}.${nodes.map { it.property.name }.joinToString(".")}"
                    throw ObjectPathException("""Failed to parse "$path": property $validSubPath.$propertyName not found.""")
                }

                nodes.add(Node(property))
                contextType = property.returnType.jvmErasure
            }

            return ObjectPath(rootType, nodes)
        }
    }
}

class ObjectPathException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

fun <T : Any> String.constructAs(type: KClass<out T>): T {
    return when (type) {
        Boolean::class -> when(this.toLowerCase()) {
            "true" -> true as T
            "false" -> false as T
            else -> throw IllegalArgumentException("Invalid boolean literal: \"$this\"")
        }
        Short::class   -> toShort() as T
        Int::class     -> toInt() as T
        Long::class    -> toLong() as T
        Float::class   -> toFloat() as T
        Double::class  -> toDouble() as T
        String::class  -> this as T
        Path::class    -> Paths.get(this) as T

        else -> throw UnsupportedOperationException("Cannot construct a ${type.simpleName} from a string")
    }
}

/**
 * Whether objects of the given type are leafs (which are not to be considered for
 * overrides).
 */
private val Type.isLeaf: Boolean
    get() {
        val asCls = toClass()
        if (asCls.isPrimitive) return true
        if (!asCls.`package`.name.startsWith("com.github.prologdb.orchestration.config")) return true
        return false
    }

private val KType.isLeaf: Boolean
    get() = this.javaType.isLeaf

private fun Type.toClass(): Class<*> {
    if (this is Class<*>) {
        return this
    }

    if (this is ParameterizedType) {
        return this.rawType as Class<*>
    }

    try {
        return Class.forName(typeName)
    } catch (ex: ClassNotFoundException) {
        throw RuntimeException("Failed to find class for type $typeName", ex)
    }

}