package com.github.prologdb.dbms.catalog

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.term.prologTypeName
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer

object SystemCatalogJacksonModule : SimpleModule("prologdb-system-catalog") {
    private val binaryWriter = BinaryPrologWriter.getDefaultInstance()
    private val binaryReader = BinaryPrologReader.getDefaultInstance()

    init {
        addSerializer(GoalSerializer)
        addDeserializer(Query::class.java, GoalDeserializer)
        addSerializer(TermSerializer)
        addDeserializer(Variable::class.java, TermDeserializer.forSubtype<Variable>())
    }

    object GoalSerializer : StdScalarSerializer<Query>(Query::class.java) {
        override fun serialize(value: Query, gen: JsonGenerator, context: SerializerProvider?) {
            val target = ByteArrayOutputStream()
            DataOutputStream(target).use {
                binaryWriter.writeQueryTo(value, it)
            }
            gen.writeBinary(target.toByteArray())
        }
    }

    object GoalDeserializer : StdScalarDeserializer<Query>(Query::class.java) {
        override fun deserialize(p: JsonParser, context: DeserializationContext?): Query {
            val target = ByteArrayOutputStream()
            p.readBinaryValue(target)
            return binaryReader.readQueryFrom(ByteBuffer.wrap(target.toByteArray()))
        }
    }

    object TermSerializer : StdScalarSerializer<Term>(Term::class.java) {
        override fun serialize(value: Term, gen: JsonGenerator, provider: SerializerProvider?) {
            val target = ByteArrayOutputStream()
            DataOutputStream(target).use {
                binaryWriter.writeTermTo(value, it)
            }
            gen.writeBinary(target.toByteArray())
        }
    }

    object TermDeserializer : StdScalarDeserializer<Term>(Term::class.java) {
        override fun deserialize(p: JsonParser, ctxt: DeserializationContext?): Term {
            val target = ByteArrayOutputStream()
            p.readBinaryValue(target)
            return binaryReader.readTermFrom(ByteBuffer.wrap(target.toByteArray()))
        }

        inline fun <reified T : Term> forSubtype() = object : JsonDeserializer<T>() {
            override fun deserialize(p: JsonParser, ctxt: DeserializationContext): T {
                val term = TermDeserializer.deserialize(p, ctxt)
                if (term is T) {
                    return term
                } else {
                    throw ctxt.instantiationException(T::class.java, "Expected a ${T::class.java.prologTypeName}, got a ${term.prologTypeName}")
                }
            }
        }
    }
}