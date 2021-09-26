package com.github.prologdb.util.metadata

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Path
import java.util.*

/**
 * Writes the data to a file
 */
class FileMetadataRepository(
    file: Path,
    private val valueObjectMapper: ObjectMapper = jacksonObjectMapper()
) : MetadataRepository {

    private val file = file.toFile()

    private val properties: Properties by lazy {
        val fromFile = Properties()
        FileInputStream(this@FileMetadataRepository.file).use { fromFile.load(it) }
        fromFile
    }

    override fun save(key: String, value: Any) {
        bulkSave(mapOf(key to value))
    }

    override fun remove(key: String) {
        properties.remove(key)
        flushToFile("last modified properties: $key")
    }

    override fun bulkSave(data: Map<String, Any>) {
        for ((key, value) in data) {
            properties.setProperty(key, valueObjectMapper.writeValueAsString(value))
        }
        flushToFile("last modified properties: ${data.keys.joinToString()}")
    }

    override fun <T : Any> load(key: String, valueClass: Class<T>): T? {
        return valueObjectMapper.readValue(properties.getProperty(key, "null"), valueClass)
    }

    private fun flushToFile(comment: String?) {
        FileOutputStream(file).use { properties.store(it, comment) }
    }
}