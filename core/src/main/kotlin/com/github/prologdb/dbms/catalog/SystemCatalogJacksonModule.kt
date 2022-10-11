package com.github.prologdb.dbms.catalog

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.runtime.query.Query
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer

object SystemCatalogJacksonModule : SimpleModule("prologdb-system-catalog") {
    init {
        addSerializer(GoalSerializer)
    }

    object GoalSerializer : StdScalarSerializer<Query>(Query::class.java) {
        private val binaryWriter = BinaryPrologWriter()
        override fun serialize(value: Query, gen: JsonGenerator, context: SerializerProvider?) {
            val target = ByteArrayOutputStream()
            binaryWriter.writeQueryTo(value, DataOutputStream(target))
            gen.writeBinary(target.toByteArray())
        }
    }

    object GoalDeserializer : StdScalarDeserializer<Query>(Query::class.java) {
        private val binaryReader = BinaryPrologReader()
        override fun deserialize(p: JsonParser, context: DeserializationContext?): Query {
            val target = ByteArrayOutputStream()
            p.readBinaryValue(target)
            return binaryReader.readQueryFrom(ByteBuffer.wrap(target.toByteArray()))
        }
    }
}