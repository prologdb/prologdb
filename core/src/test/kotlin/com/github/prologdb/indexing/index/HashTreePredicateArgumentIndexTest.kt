package com.github.prologdb.indexing.index

import com.github.prologdb.LongRunning
import com.github.prologdb.Performance
import com.github.prologdb.indexing.standardDeviation
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.term.Atom
import io.kotlintest.matchers.*
import io.kotlintest.specs.FreeSpec
import java.math.BigInteger
import java.util.*

class HashTreePredicateArgumentIndexTest : FreeSpec() {
    override val oneInstancePerTest = true
init {
    val index = AtomIndex()

    "functional" - {
        "insert and retrieve" - {
            "single item" {
                index.onInserted(Atom("atom"), 152)

                val foundIndexes = index.find(Atom("atom")).toList()

                foundIndexes shouldEqual listOf(152L)
            }

            "multiple items - disjoint at first element" {
                index.onInserted(Atom("atom"), 152L)
                index.onInserted(Atom("foobar"), 7781L)

                val indexesForAtom = index.find(Atom("atom")).toList()
                val indexesForFoobar = index.find(Atom("foobar")).toList()

                indexesForAtom shouldEqual listOf(152L)
                indexesForFoobar shouldEqual listOf(7781L)
            }

            "same item at different indexes" {
                index.onInserted(Atom("foo"), 924L)
                index.onInserted(Atom("foo"), 1252L)
                index.onInserted(Atom("foo"), 22792L)
                index.onInserted(Atom("foo"), 45621L)
                index.onInserted(Atom("foo"), 97612L)

                val foundIndexes = index.find(Atom("foo")).toSet()

                foundIndexes shouldEqual setOf(97612L, 1252L, 924L, 45621L, 22792L)
            }
        }

        "removal" - {
            "add and remove - single item" {
                index.onInserted(Atom("foo"), 1222)

                index.find(Atom("foo")).toSet() shouldNot beEmpty()

                index.onRemoved(Atom("foo"), 1222)

                index.find(Atom("foo")).toSet() should beEmpty()
            }

            "add and remove - same item on different indexes" {
                index.onInserted(Atom("foo"), 1222L)
                index.onInserted(Atom("foo"), 5222L)

                index.find(Atom("foo")).toSet().size shouldEqual 2

                index.onRemoved(Atom("foo"), 5222L)

                index.find(Atom("foo")).toSet() shouldEqual setOf(1222L)
            }
        }
    }

    "performance" - {
        val random = Random()
        fun randomString(): String {
            val buffer = ByteArray(random.nextInt(15) + 3, { 0 })
            random.nextBytes(buffer)
            return BigInteger(buffer).toString(36)
        }

        "reading should be constant time" {
            // SETUP
            val index = AtomIndex()
            val allData = Array(50000, { Atom(randomString()) })
            for (i in 0 .. allData.lastIndex) {
                index.onInserted(allData[i], i.toLong())
            }

            // ACT
            val nMeasures = 15
            val nMeasuresAsWarmup = 10
            val nReadsPerMeasure = 50
            val measures = mutableListOf<Long>()
            for (iMeasure in 0 until nMeasures + nMeasuresAsWarmup) {
                val atomsToRead = Array<Atom>(nReadsPerMeasure, { allData[random.nextInt(allData.lastIndex)] })
                val startedAt = System.nanoTime()
                for (atomToRead in atomsToRead) {
                    index.find(atomToRead)
                }
                val durationNanos = System.nanoTime() - startedAt

                if (iMeasure >= nMeasuresAsWarmup) {
                    measures.add(durationNanos)
                }
            }

            // ASSERT
            val avg = measures.average()
            val twoSigma = 2 * measures.standardDeviation
            measures.filter { it < avg - twoSigma || it > avg + twoSigma }.standardDeviation should beLessThanOrEqualTo(100000.0)
        }

        "insertion - append only should be constant time" {
            val index = AtomIndex()

            val batchSize = 50
            val nBatches = 200
            val nWarumpBatches = 20
            var nItem = 0
            val batchTimesMicros = mutableListOf<Double>()

            for (nBatch in 1 .. nBatches + nWarumpBatches) {
                val batchStartedAt = System.nanoTime()
                for (nItemInBatch in 1 .. batchSize) {
                    index.onInserted(Atom(randomString()), nItem++.toLong())
                }

                if (nBatch > nWarumpBatches) {
                    batchTimesMicros.add(((System.nanoTime() - batchStartedAt) / 1000).toDouble())
                }
            }

            val avg = batchTimesMicros.average()
            val twoSigma = 2 * batchTimesMicros.standardDeviation
            batchTimesMicros.filter { it > avg + twoSigma || it < avg - twoSigma }.standardDeviation should beLessThanOrEqualTo(800.0)
        }.config(tags = setOf(LongRunning, Performance))
    }
}}

private class AtomIndex : HashTreePredicateArgumentIndex<Atom, Char>(Atom::class) {
    override fun getNumberOfElementsIn(value: Atom): Int = value.name.length

    override fun getElementAt(value: Atom, index: Int): Char = value.name[index]

    override fun hashElement(element: Char): Int = element.hashCode()

    override fun elementsEqual(a: Char, b: Char): Boolean = a == b
}