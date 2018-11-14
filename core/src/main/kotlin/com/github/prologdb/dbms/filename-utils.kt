package com.github.prologdb.dbms

/**
 * All characters in the string except `A-Z`, `0-9`, `_`
 * and `.` are replaced with an url-encoding like escape sequence:
 * it is prefixed with a `-` (hyphen) followed by the
 * 2-by hexadecimal encoded UNICODE value for the character.
 */
fun String.toSaveFileName(): String {
    val originalChars = toCharArray()
    val indicesToReplace = (0..lastIndex).filter { !this[it].isLetterOrDigit() && this[it] != '_' && this[it] != '.' }

    if (indicesToReplace.isEmpty()) return this
    val targetChars = CharArray(originalChars.size + indicesToReplace.size * 4) { '\u0000' }

    var sourcePointer = 0
    var targetPointer = 0
    for (indexToReplace in indicesToReplace) {
        val partLength = indexToReplace - sourcePointer
        System.arraycopy(originalChars, sourcePointer, targetChars, targetPointer, partLength)
        sourcePointer += partLength
        targetPointer += partLength

        val charUnicode = originalChars[sourcePointer++].toInt()
        val charUnicodeString = ((charUnicode and 0xFFFF) or 0x10000).toString(16)
        // the or 0x10000 force the output to contain leading zeroes

        targetChars[targetPointer++] = '-'
        targetChars[targetPointer++] = charUnicodeString[1]
        targetChars[targetPointer++] = charUnicodeString[2]
        targetChars[targetPointer++] = charUnicodeString[3]
        targetChars[targetPointer++] = charUnicodeString[4]
    }

    System.arraycopy(originalChars, sourcePointer, targetChars, targetPointer, originalChars.size - sourcePointer)

    return String(targetChars)
}

/**
 * Reverses [toSaveFileName]
 */
fun String.fromSaveFileName(): String {
    val originalChars = toCharArray()
    val sequenceStartIndices = (0..lastIndex).filter { this[it] == '-' }

    if (sequenceStartIndices.isEmpty()) return this

    val targetChars = CharArray(originalChars.size - sequenceStartIndices.size * 4) { '\u0000' }

    var sourcePointer = 0
    var targetPointer = 0
    for (sequenceStartIndex in sequenceStartIndices) {
        val partLength = sequenceStartIndex - sourcePointer
        System.arraycopy(originalChars, sourcePointer, targetChars, targetPointer, partLength)
        sourcePointer += partLength
        targetPointer += partLength

        // skip hyphen and data
        sourcePointer += 5
        val unicodeHexadecimalString = this.substring(sequenceStartIndex + 1, sequenceStartIndex + 5)
        val unicode = unicodeHexadecimalString.toIntOrNull(16) ?: throw IllegalArgumentException("Illegal sequence at offset ${sourcePointer - 5}: $unicodeHexadecimalString is not hexadecimal.")
        targetChars[targetPointer++] = unicode.toChar()
    }

    System.arraycopy(originalChars, sourcePointer, targetChars, targetPointer, originalChars.size - sourcePointer)

    return String(targetChars)
}