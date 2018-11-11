package com.github.prologdb.orchestration.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlin.reflect.KClass

private val configObjectMapper = {
    val m = ObjectMapper(YAMLFactory())
    m.registerKotlinModule()
    m.propertyNamingStrategy = PropertyNamingStrategy.KEBAB_CASE
    m
}()

fun <C : Any> String.parseAsConfig(cclass: KClass<C>): C = configObjectMapper.readValue(this, cclass.java)
inline fun <reified C : Any> String.parseAsConfig(): C = parseAsConfig(C::class)