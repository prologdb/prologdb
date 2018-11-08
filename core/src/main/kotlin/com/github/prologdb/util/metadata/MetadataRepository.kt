package com.github.prologdb.util.metadata

/**
 * A simple key-value store for other objects (database internal ones) to persist metadata.
 * Metadata must be saved as JSON; implementations mus use Jackson to convert to JSON.
 */
interface MetadataRepository {
    /**
     * Persists the given value in association with the given key. If
     * A value already exists for the given key, it will be overwritten.
     * @param key Allowed characters: alphanumeric plus `.` and `_`
     * @param asType The type to write as (if `value` is a subtype)
     */
    fun save(key: String, value: Any)

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
}

inline fun <reified T : Any> MetadataRepository.load(key: String): T? = load(key, T::class.java)

inline operator fun <reified T: Any> MetadataRepository.get(key: String): T? = load(key)

operator fun MetadataRepository.set(key: String, value: Any) = save(key, value)