package com.github.prologdb.storage

import java.io.DataInput
import java.io.DataOutput
import java.nio.ByteBuffer
import kotlin.reflect.KClass
import kotlin.reflect.KParameter
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.jvmErasure

inline fun <reified T: Any> ByteBuffer.readStruct(): T = readStruct(T::class)

fun <T : Any> ByteBuffer.readStruct(cls: KClass<T>): T {
    rejectUnlessStructClass(cls)

    val primaryConstructor = cls.primaryConstructor!!

    val parameterValues: Map<KParameter, Any> = primaryConstructor.parameters
        .filterNot { it.isDeclaredTransient }
        .map { param ->
            Pair(param, when (param.type.jvmErasure) {
                Boolean::class -> this.readByte() != 0.toByte()
                Byte::class -> this.readByte()
                Short::class -> this.readShort()
                Int::class -> this.readInt()
                Long::class -> this.readLong()
                else -> throw RuntimeException("Erroneous code")
            })
        }
        .toMap()

    return primaryConstructor.callBy(parameterValues)
}

inline fun <reified T: Any> DataInput.readStruct(): T = readStruct(T::class)

fun <T : Any> DataInput.readStruct(cls: KClass<T>): T {
    rejectUnlessStructClass(cls)

    val primaryConstructor = cls.primaryConstructor!!

    val parameterValues: Map<KParameter, Any> = primaryConstructor.parameters
        .filterNot { it.isDeclaredTransient }
        .map { param ->
            Pair(param, when (param.type.jvmErasure) {
                Boolean::class -> this.readByte() != 0.toByte()
                Byte::class -> this.readByte()
                Short::class -> this.readShort()
                Int::class -> this.readInt()
                Long::class -> this.readLong()
                else -> throw RuntimeException("Erroneous code")
            })
        }
        .toMap()

    return primaryConstructor.callBy(parameterValues)
}

fun <T : Any> DataOutput.writeStruct(struct: T) {
    val cls: KClass<T> = struct::class as KClass<T>
    rejectUnlessStructClass(cls)

    cls.primaryConstructor!!.parameters
        .filterNot { it.isDeclaredTransient }
        .forEach { cparameter ->
            val member = cls.declaredMemberProperties.first { it.name == cparameter.name }
            val value = member.get(struct) ?: throw RuntimeException("Erroneous code")
            when (value) {
                is Boolean -> writeByte(if (value) 1 else 0)
                is Byte -> writeByte(value.toInt())
                is Short -> writeShort(value.toInt())
                is Int -> writeInt(value)
                is Long -> writeLong(value)
                else -> throw RuntimeException("Erroneous code")
            }
        }
}

fun <T : Any> ByteBuffer.writeStruct(struct: T) {
    val cls: KClass<T> = struct::class as KClass<T>
    rejectUnlessStructClass(cls)

    cls.primaryConstructor!!.parameters
        .filterNot { it.isDeclaredTransient }
        .forEach { cparameter ->
            val member = cls.declaredMemberProperties.first { it.name == cparameter.name }
            val value = member.get(struct) ?: throw RuntimeException("Erroneous code")
            when (value) {
                is Boolean -> writeByte(if (value) 1 else 0)
                is Byte -> writeByte(value)
                is Short -> writeShort(value)
                is Int -> writeInt(value)
                is Long -> writeLong(value)
                else -> throw RuntimeException("Erroneous code")
            }
        }
}

private fun ByteBuffer.readByte(): Byte = get()
private fun ByteBuffer.readShort(): Short = ((get().toInt() and 0xFF shl 8) or (get().toInt() and 0xFF)).toShort()
private fun ByteBuffer.readInt(): Int {
    val a = get().toInt() and 0xFF
    val b = get().toInt() and 0xFF
    val c = get().toInt() and 0xFF
    val d = get().toInt() and 0xFF

    return (a shl 24) or (b shl 16) or (c shl 8) or d
}
private fun ByteBuffer.readLong(): Long = readInt().toLong() shl 32 or readInt().toLong()

private fun ByteBuffer.writeByte(e: Byte) = put(e)
private fun ByteBuffer.writeShort(e: Short) {
    val eInt = e.toInt()
    put((eInt ushr 8).toByte())
    put(eInt.toByte())
}
private fun ByteBuffer.writeInt(e: Int) {
    put((e ushr 24).toByte())
    put((e ushr 16).toByte())
    put((e ushr  8).toByte())
    put(e.toByte())
}
private fun ByteBuffer.writeLong(e: Long) {
    writeInt((e ushr 32).toInt())
    writeInt(e.toInt())
}

private val validationCache = mutableMapOf<KClass<*>, Exception?>()

/**
 * Throws an exception if cls does not fulfill all of these properties:
 * * is a data class
 * * has a primary constructor
 * * every parameter to the primary constructor...
 *   * is of type boolean, byte, short, int, long
 *   * is not-nullable
 *   * is declared [Transient] and optional OR is declared as a field (var or val)
 */
private fun rejectUnlessStructClass(cls: KClass<*>) {
    if (validationCache.containsKey(cls)) {
        val ex = validationCache[cls]
        if (ex != null) throw ex
        return
    }

    try {
        if (!cls.isData) throw IllegalArgumentException("The given class is not a data class")

        val primaryConstructor = cls.primaryConstructor ?: throw IllegalArgumentException("The given class does not have a primary constructor")
        primaryConstructor.parameters
            .forEach { cparameter ->
                if (cparameter.type.jvmErasure !in setOf(Boolean::class, Byte::class, Short::class, Int::class, Long::class)) {
                    throw IllegalArgumentException("Unsupported type ${cparameter.type} for parameter ${cparameter.name}")
                }

                if (cparameter.type.isMarkedNullable) {
                    throw IllegalArgumentException("Nullable type ${cparameter.type} for parameter ${cparameter.name} is not supported")
                }

                if (cparameter.isDeclaredTransient) {
                    if (!cparameter.isOptional) {
                        throw IllegalArgumentException("Parameter ${cparameter.name} of ${cls.qualifiedName} is declared transient, must be optional")
                    }
                } else if (cls.declaredMemberProperties.none { it.name == cparameter.name }) {
                    throw IllegalArgumentException("Primary constructor parameter ${cparameter.name} does not map to a declared member of ${cls.qualifiedName}")
                }
            }
    }
    catch (ex: Exception) {
        validationCache[cls] = ex
        throw ex
    }

    validationCache[cls] = null
}

private val KParameter.isDeclaredTransient: Boolean
    get() = this.annotations.any { it.annotationClass.isSubclassOf(Transient::class) }