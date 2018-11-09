package com.github.prologdb.util.metadata

import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldEqual
import io.kotlintest.matchers.shouldThrow
import io.kotlintest.specs.FreeSpec

class InMemoryMetadataRepositoryTest : FreeSpec({
    "store and retrieve" {
        val repo = InMemoryMetadataRepository()
        repo.save("Foo", "Bar")

        repo.load("Foo", String::class.java) shouldEqual "Bar"
    }

    "store and load with wrong type" {
        val repo = InMemoryMetadataRepository()
        repo.save("foo", "bar")

        shouldThrow<RuntimeException> {
            repo.load("foo", Long::class.java)
        }
    }

    "load non existing" {
        val repo = InMemoryMetadataRepository()

        repo.load("key", String::class.java) shouldBe null
    }
})
