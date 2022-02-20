package com.github.prologdb.net.util

import com.github.prologdb.runtime.PrologException

fun PrologException.prettyPrint(): String {
    val b = StringBuilder()
    prettyPrint(b)
    return b.toString()
}

fun PrologException.prettyPrint(toBuilder: StringBuilder) {
    toBuilder.append("M: ")
    toBuilder.append(message ?: "null")
    toBuilder.append("\n")
    prettyPrintStackTrace(toBuilder)
}

fun PrologException.prettyPrintStackTrace(): String {
    val b = StringBuilder()
    prettyPrintStackTrace(b)
    return b.toString()
}

fun PrologException.prettyPrintStackTrace(toBuilder: StringBuilder) {
    for (sf in prologStackTrace) {
        toBuilder.append("\t")
        toBuilder.append(sf.toString())
        toBuilder.append("\n")
    }

    val _cause = cause
    if (_cause != null && _cause is PrologException) {
        toBuilder.append("caused by: ")
        _cause.prettyPrint(toBuilder)
    }

    suppressed
        .asSequence()
        .filterIsInstance<PrologException>()
        .forEach {
            toBuilder.append("suppressed: ")
            it.prettyPrint(toBuilder)
        }
}

// TODO: correct typo
fun Int.unsingedIntHexString(): String = toLong().and(0xFFFFFFFFL).toString(16)