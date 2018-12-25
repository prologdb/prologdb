package com.github.prologdb.util.metadata

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.io.BufferedWriter
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.OutputStreamWriter
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

    override fun bulkSave(data: Map<String, Any>) {
        for ((key, value) in data) {
            properties.setProperty(key, valueObjectMapper.writeValueAsString(value))
        }
        FileOutputStream(file).use { properties.storeCleanly(it) }
    }

    override fun <T : Any> load(key: String, valueClass: Class<T>): T? {
        return valueObjectMapper.readValue(properties.getProperty(key, "null"), valueClass)
    }
}

/**
 * Like [Properties.store] but without comment.
 */
private fun Properties.storeCleanly(out: FileOutputStream) {
    val writer = BufferedWriter(OutputStreamWriter(out, "8859_1"))
    synchronized(this) {
        for (keyUntyped in this.keys) {
            val key = keyUntyped as String
            val value = get(key) as String
            writer.write(key.propertiesEscape(escapeSpace = true) + "=" + value.propertiesEscape(escapeSpace = false))
            writer.newLine()
        }
    }
}

private val HEX_DIGITS = arrayOf(
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'A', 'B', 'C', 'D', 'E', 'F'
)

/*
 * Converts unicodes to encoded &#92;uxxxx and escapes
 * special characters with a preceding slash
 */
private fun String.propertiesEscape(escapeSpace: Boolean): String {
    val len = this.length
    var bufLen = len * 2
    if (bufLen < 0) {
        bufLen = Integer.MAX_VALUE
    }
    val outBuffer = StringBuffer(bufLen)

    for (x in 0 until len) {
        val aChar = this[x]
        // Handle common case first, selecting largest block that
        // avoids the specials below
        if (aChar.toInt() in 62..126) {
            if (aChar == '\\') {
                outBuffer.append('\\')
                outBuffer.append('\\')
                continue
            }
            outBuffer.append(aChar)
            continue
        }
        when (aChar) {
            ' ' -> {
                if (x == 0 || escapeSpace)
                    outBuffer.append('\\')
                outBuffer.append(' ')
            }
            '\t' -> {
                outBuffer.append('\\')
                outBuffer.append('t')
            }
            '\n' -> {
                outBuffer.append('\\')
                outBuffer.append('n')
            }
            '\r' -> {
                outBuffer.append('\\')
                outBuffer.append('r')
            }
            '\u000C' -> { // form feed \f
                outBuffer.append('\\')
                outBuffer.append('f')
            }
            '=', ':', '#', '!' -> {
                outBuffer.append('\\')
                outBuffer.append(aChar)
            }
            else -> if (aChar.toInt() < 0x0020 || aChar.toInt() > 0x007e) {
                outBuffer.append('\\')
                outBuffer.append('u')
                outBuffer.append(HEX_DIGITS[aChar.toInt() shr 12 and 0xF])
                outBuffer.append(HEX_DIGITS[aChar.toInt() shr 8 and 0xF])
                outBuffer.append(HEX_DIGITS[aChar.toInt() shr 4 and 0xF])
                outBuffer.append(HEX_DIGITS[aChar.toInt() and 0xF])
            } else {
                outBuffer.append(aChar)
            }
        }
    }
    return outBuffer.toString()
}