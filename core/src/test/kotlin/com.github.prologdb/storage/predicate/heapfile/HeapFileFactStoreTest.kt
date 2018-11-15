package com.github.prologdb.storage.predicate.heapfile

import com.github.prologdb.async.IrrelevantPrincipal
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.storage.heapfile.HeapFile
import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldThrow
import io.kotlintest.specs.FreeSpec
import java.io.File

class HeapFileFactStoreTest : FreeSpec({
    val tmpFile = File.createTempFile("hfpst", "")
    tmpFile.deleteOnExit()
    HeapFile.initializeForContiguousDevice(
        tmpFile.toPath()
    )

    val store = HeapFileFactStore(
        ClauseIndicator.of("foo", 3),
        BinaryPrologReader.getDefaultInstance(),
        BinaryPrologWriter.getDefaultInstance(),
        HeapFile.forExistingFile(tmpFile.toPath())
    )

    val abc = arrayOf(Atom("a"), Atom("b"), Atom("c"))

    "write succeeds" {
        store.store(IrrelevantPrincipal, Predicate("foo", abc))
            .get()
    }

    "write of predicate with wrong name fails" {
        shouldThrow<IllegalArgumentException> {
            store.store(IrrelevantPrincipal, Predicate("bar", abc))
                .get()
        }
    }

    "write of predicate with wrong arity fails" {
        shouldThrow<IllegalArgumentException> {
            store.store(IrrelevantPrincipal, Predicate("foo", arrayOf(Atom("a"))))
        }
    }

    "write and read back" {
        val original = Predicate("foo", abc)
        val pid = store.store(IrrelevantPrincipal, original)
            .get()

        val loaded = store.retrieve(IrrelevantPrincipal, pid).get()
        loaded shouldBe original
    }
}) {
    override val oneInstancePerTest = true
}