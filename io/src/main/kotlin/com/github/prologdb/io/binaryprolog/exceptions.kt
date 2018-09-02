package com.github.prologdb.io.binaryprolog

sealed class BinaryPrologIOException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

class BinaryPrologSerializationException(message: String, cause: Throwable? = null) : BinaryPrologIOException(message, cause)

class BinaryPrologDeserializationException(message: String, cause: Throwable? = null) : BinaryPrologIOException(message, cause)