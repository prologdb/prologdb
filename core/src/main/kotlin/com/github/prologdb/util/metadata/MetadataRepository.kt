package com.github.prologdb.util.metadata

import kotlin.reflect.KProperty

/**
 * A simple key-value store for other objects (database internal ones) to persist metadata.
 * Metadata must be saved as JSON; implementations must use Jackson to convert to JSON.
 */
interface MetadataRepository {
    /**
     * Persists the given value in association with the given key. If
     * A value already exists for the given key, it will be overwritten.
     * @param key Allowed characters: alphanumeric plus `.` and `_`
     * @param asType The type to write as (if `value` is a subtype)
     */
    fun save(key: String, value: Any)

    fun remove(key: String)

    /**
     * Saves all the given associations. This can be optimized for
     * persistent stores: reading all the persistent data, modifying it
     * in-memory and then writing it back out.
     *
     * The default implementation defers to [save].
     */
    fun bulkSave(data: Map<String, Any>) {
        for ((key, value) in data) save(key, value)
    }

    /**
     * Loads the value for the given key.
     * @return The value or `null` if no value is associated with the given key
     * @throws RuntimeException If the persisted JSON cannot be read back as a value of the given class.
     */
    fun <T : Any> load(key: String, valueClass: Class<T>): T?

    fun <T : Any> property(key: String, type: Class<T>) = MetadataRepositoryNamedDelegate(this, key, type)

    fun <T: Any> property(type: Class<T>) = MetadataRepositoryDelegate(this, type)
}

inline fun <reified T : Any> MetadataRepository.load(key: String): T? = load(key, T::class.java)

inline operator fun <reified T: Any> MetadataRepository.get(key: String): T? = load(key)

operator fun MetadataRepository.set(key: String, value: Any) = save(key, value)

inline fun <reified T: Any> MetadataRepository.property(key: String) = property(key, T::class.java)

inline fun <reified T: Any> MetadataRepository.property() = property(T::class.java)

class MetadataRepositoryNamedDelegate<T : Any>(val repository: MetadataRepository, val key: String, val type: Class<T>) {
    operator fun getValue(thisRef: Any?, property: KProperty<*>): T? = repository.load(key, type)
    operator fun setValue(thisRef: Any?, property: KProperty<*>, value: T?) {
        if (value != null) {
            repository.save(key, value)
        } else {
            repository.remove(key)
        }
    }
}

class MetadataRepositoryDelegate<T : Any>(val repository: MetadataRepository, val type: Class<T>) {
    operator fun getValue(thisRef: Any?, property: KProperty<*>): T? = repository.load(property.name, type)
    operator fun setValue(thisRef: Any?, property: KProperty<*>, value: T?) {
        if (value != null) {
            repository.save(property.name, value)
        } else {
            repository.remove(property.name)
        }
    }
}