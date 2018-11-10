package com.github.prologdb.orchestration.config

import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.jvmErasure

/**
 * Given an instance of a data class: creates a deep clone including
 * the given overrides.
 * @receiver Assumes immutability
 */
infix fun <T : Any> T.plusOverrides(overrides: Map<ObjectPath<T>, String>): T {
    assert(overrides.keys.map { it.rootType }.distinct().size == 1)

    return plusOverridesInternal(overrides as Map<ObjectPath<Any>, String>)
}

private fun <T: Any> T.plusOverridesInternal(overrides: Map<ObjectPath<Any>, String>): T {
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
        val value = onThisLevel[ctorParam.name!!]?.constructAs(ctorParam.type.javaClass)
            ?: property.get(this)

        val overridesForValue = onNextLevel
            .filter { it.key.firstNode.property == property }
            .map { Pair(it.key.subPath(1), it.value) }
            .toMap()

        return@Array value?.plusOverridesInternal(overridesForValue)
    }

    return thisLevelCtor.call(*invocationParams) as T
}

private fun <T> String.constructAs(type: Class<T>): T {
    TODO()
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
     * @return a copy of this object path without the first `n` nodes.
     */
    fun subPath(n: Int): ObjectPath<Any> {
        assert(n >= 0)
        assert(n < nodes.size)
        val subList = nodes.subList(n, nodes.size)
        val firstNode = subList.first()
        val contextType = firstNode.property.returnType.jvmErasure as KClass<Any>
        return ObjectPath(contextType, subList)
    }

    data class Node(val property: KProperty<*>)

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
                    val validSubPath = "#${contextType.simpleName}.${nodes.map { it.property.name }.joinToString(".")}"
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