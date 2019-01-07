package com.github.prologdb.orchestration.config

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.reflect.KClass

private val configObjectMapper = {
    val m = ObjectMapper(YAMLFactory())
    m.registerKotlinModule()
    m.registerModule(RelativePathModule)
    m.propertyNamingStrategy = PropertyNamingStrategy.KEBAB_CASE
    m
}()

private object RelativePathModule : SimpleModule("RelativePath") {
    var relativeTo: Path = Paths.get(".").toAbsolutePath()
        set(value) {
            if (!value.isAbsolute) throw IllegalArgumentException("Relation path must be absolute")
            field = value
        }

    init {
        addDeserializer(Path::class.java, object : JsonDeserializer<Path>()  {
            override fun deserialize(parser: JsonParser, ctxt: DeserializationContext?): Path? {
                val string = parser.text ?: return null
                val path = Paths.get(string)
                return if (path.isAbsolute) path else relativeTo.resolve(path).normalize()
            }
        })
    }
}

fun <C : Any> String.parseAsConfig(cclass: KClass<C>, allPathsRelativeTo: Path): C {
    synchronized(configObjectMapper) {
        RelativePathModule.relativeTo = allPathsRelativeTo
        return configObjectMapper.readValue(this, cclass.java)
    }
}
inline fun <reified C : Any> String.parseAsConfig(allPathsRelativeTo: Path): C = parseAsConfig(C::class, allPathsRelativeTo)