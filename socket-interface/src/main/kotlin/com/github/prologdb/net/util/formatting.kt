package com.github.prologdb.net.util

import com.github.prologdb.runtime.PrologRuntimeException

fun PrologRuntimeException.prettyPrint(): String {
    val b = StringBuilder()
    prettyPrint(b)
    return b.toString()
}

fun PrologRuntimeException.prettyPrint(toBuilder: StringBuilder) {
    toBuilder.append("M: ")
    toBuilder.append(message ?: "null")
    toBuilder.append("\n")
    for (sf in prologStackTrace) {
        toBuilder.append("\t")
        toBuilder.append(sf.toString())
        toBuilder.append("\n")
    }

    val _cause = cause
    if (_cause != null && _cause is PrologRuntimeException) {
        toBuilder.append("caused by: ")
        _cause.prettyPrint(toBuilder)
    }

    (suppressed
        .asSequence()
        .filter { it is PrologRuntimeException } as Sequence<PrologRuntimeException>)
        .forEach {
            toBuilder.append("suppressed: ")
            it.prettyPrint(toBuilder)
        }
}

// TODO: correct typo
fun Int.unsingedIntHexString(): String = toLong().and(0xFFFFFFFFL).toString(16)