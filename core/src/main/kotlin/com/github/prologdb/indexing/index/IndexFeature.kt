package com.github.prologdb.indexing.index

import com.github.prologdb.indexing.PredicateArgumentIndex
import com.github.prologdb.indexing.RangeQueryPredicateArgumentIndex
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

/**
 * Models features of an index. These are used when creating new indexes and inspecting existing ones:
 * * when creating, users can specify features/properties. the application an then choose the best implementation
 * * when querying, the query optimizer can optimize the query based on the features of the available indexes
 */
enum class IndexFeature(val predicate: (KClass<out PredicateArgumentIndex>) -> Boolean) {
    /** The index can read entries in O(n) time where n is the number of affected rows */
    CONSTANT_TIME_READ(hasAnnotation(ConstantTimeRead::class)),

    /** The index can write entries in O(1) time */
    CONSTANT_TIME_WRITE(hasAnnotation(ConstantTimeWrite::class)),

    /** The index' data structure allows for range-queries that are not index scans */
    EFFICIENT_RANGE_QUERIES(implementsInterface(RangeQueryPredicateArgumentIndex::class));
}

/**
 * Marker annotation for [PredicateArgumentIndex]es that can read in O(n) time where n is the number of affected
 * entries.
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS)
annotation class ConstantTimeRead

/**
 * Marker annotation for [PredicateArgumentIndex]ex that can write entries in O(1) time
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS)
annotation class ConstantTimeWrite

private fun implementsInterface(intf: KClass<*>): (KClass<*>) -> Boolean
        = { it.isSubclassOf(intf) }

private fun hasAnnotation(annotationCls: KClass<out Annotation>): (KClass<*>) -> Boolean
        = { cls -> cls.annotations.any { it.annotationClass.isSubclassOf(annotationCls) }}