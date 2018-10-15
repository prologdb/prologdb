package com.github.prologdb.net

fun Int.unsingedIntHexString(): String = toLong().and(0xFFFFFFFFL).toString(16)