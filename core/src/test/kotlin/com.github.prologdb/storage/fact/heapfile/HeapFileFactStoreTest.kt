package com.github.prologdb.storage.fact.heapfile

import com.github.prologdb.async.IrrelevantPrincipal
import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.storage.heapfile.HeapFile
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.IsolationMode
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import java.io.File

class HeapFileFactStoreTest : FreeSpec({
    val tmpFile = File.createTempFile("hfpst", "")
    tmpFile.deleteOnExit()
    HeapFile.initializeForContiguousDevice(
        tmpFile.toPath()
    )

    val store = HeapFileFactStore(
        3,
        BinaryPrologReader.getDefaultInstance(),
        BinaryPrologWriter.getDefaultInstance(),
        HeapFile.forExistingFile(tmpFile.toPath())
    )

    val abc = arrayOf(Atom("a"), Atom("b"), Atom("c"))

    "write succeeds" {
        store.store(IrrelevantPrincipal, abc)
            .get()
    }

    "write of fact with wrong functor fails" {
        shouldThrow<IllegalArgumentException> {
            store.store(IrrelevantPrincipal, abc)
                .get()
        }
    }

    "write of fact with wrong arity fails" {
        shouldThrow<IllegalArgumentException> {
            store.store(IrrelevantPrincipal, arrayOf(Atom("a")))
        }
    }

    "write and read back" {
        val original = abc
        val pid = store.store(IrrelevantPrincipal, original)
            .get()

        val loaded = store.retrieve(IrrelevantPrincipal, pid).get()
        loaded shouldBe original
    }
}) {
    override fun isolationMode() = IsolationMode.InstancePerTest
}