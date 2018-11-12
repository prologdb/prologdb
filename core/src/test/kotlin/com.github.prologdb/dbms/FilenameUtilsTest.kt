package com.github.prologdb.dbms

import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldThrow
import io.kotlintest.specs.FreeSpec

class FilenameUtilsTest : FreeSpec({
    "toSaveFilename" - {
        "alphanumeric" {
            "foobar123".toSaveFileName() shouldBe "foobar123"
        }

        "alphanumeric plus underscores" {
            "foo_bar_123".toSaveFileName() shouldBe "foo_bar_123"
        }

        "with-hyphen" {
            "foo-bar".toSaveFileName() shouldBe "foo-002dbar"
        }

        "with-weirdos" {
            "foo\$bar#1/2*3".toSaveFileName() shouldBe "foo-0024bar-00231-002f2-002a3"
        }
    }

    "fromSaveFileName" - {
        "alphanumeric" {
            "foobar123".fromSaveFileName() shouldBe "foobar123"
        }

        "alphanumeric plus underscores" {
            "foo_bar_123".fromSaveFileName() shouldBe "foo_bar_123"
        }

        "with-hyphen" {
            "foo-002dbar".fromSaveFileName() shouldBe "foo-bar"
        }

        "with-weirdos" {
            "foo-0024bar-00231-002f2-002a3".fromSaveFileName() shouldBe "foo\$bar#1/2*3"
        }

        "illegal sequence" {
            shouldThrow<IllegalArgumentException> {
                "foo-002k".fromSaveFileName()
            }
        }
    }
})