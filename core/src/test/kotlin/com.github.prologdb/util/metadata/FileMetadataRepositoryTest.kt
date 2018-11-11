package com.github.prologdb.util.metadata

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.FreeSpec
import java.io.File

class FileMetadataRepositoryTest : FreeSpec({
    val tmpFile = File.createTempFile("fmdt", "")
    tmpFile.deleteOnExit()

    val repo = FileMetadataRepository(tmpFile.toPath(), jacksonObjectMapper())

    "save single" {
        repo.save("foo", TestObj("bar", listOf("baz", "bang", "fuzz")))
    }

    "save bulk" {
        repo.bulkSave(mapOf(
            "baz" to TestObj("foo", listOf("tadaa", "bang")),
            "joke" to "fun"
        ))
    }

    "save and load back" - {
        "primitive" - {
            "string" {
                repo.save("key", "value")
                repo.load<String>("key") shouldBe "value"
            }

            "double" {
                repo.save("a", 13122.221344)
                repo.load<Double>("a") shouldBe 13122.221344
            }
        }

        "complex" {
            val original = TestObj("bar", listOf("baz", "bang", "fuzz"))
            repo.save("x.y", original)
            val loaded = repo.load<TestObj>("x.y")

            loaded shouldBe original
        }
    }
}) {
    override val oneInstancePerTest = true
}

private data class TestObj(
    val a: String,
    val b: List<String>
)