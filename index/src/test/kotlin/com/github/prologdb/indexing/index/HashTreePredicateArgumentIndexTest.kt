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

                foundIndexes shouldEqual listOf(152)
            }

            "multiple items - disjoint at first element" {
                index.onInserted(Atom("atom"), 152)
                index.onInserted(Atom("foobar"), 7781)

                val indexesForAtom = index.find(Atom("atom")).toList()
                val indexesForFoobar = index.find(Atom("foobar")).toList()

                indexesForAtom shouldEqual listOf(152)
                indexesForFoobar shouldEqual listOf(7781)
            }

            "same item at different indexes" {
                index.onInserted(Atom("foo"), 924)
                index.onInserted(Atom("foo"), 1252)
                index.onInserted(Atom("foo"), 22792)
                index.onInserted(Atom("foo"), 45621)
                index.onInserted(Atom("foo"), 97612)

                val foundIndexes = index.find(Atom("foo")).toSet()

                foundIndexes shouldEqual setOf(97612, 1252, 924, 45621, 22792)
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
                index.onInserted(Atom("foo"), 1222)
                index.onInserted(Atom("foo"), 5222)

                index.find(Atom("foo")).toSet().size shouldEqual 2

                index.onRemoved(Atom("foo"), 5222)

                index.find(Atom("foo")).toSet() shouldEqual setOf(1222)
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

        "reading versus table scan" {
            // compare indexing against table scans - performance improvement is measured relatively

            // SETUP
            val index = AtomIndex()
            val allData = Array(50000, { Atom(randomString()) })
            for (i in 0 .. allData.lastIndex) {
                index.onInserted(allData[i], i)
            }

            // ACT
            val nMeasures = 15
            val nMeasuresAsWarmup = 2
            val nReadsPerMeasure = 300
            val randomVarsScope = RandomVariableScope()
            val measures = Array<Pair<Long,Long>?>(nMeasures, { null })
            for (iMeasure in 0 until nMeasures + nMeasuresAsWarmup) {
                val atomsToRead = Array<Atom>(nReadsPerMeasure, { allData[random.nextInt(allData.lastIndex)] })
                val indexStartedAt = System.nanoTime()
                for (atomToRead in atomsToRead) {
                    for (atomIndex in index.find(atomToRead)) {
                        atomToRead.unify(allData[atomIndex])
                    }
                }
                val indexDurationNanos = System.nanoTime() - indexStartedAt

                val scanStartedAt = System.nanoTime()
                for (atomToRead in atomsToRead) {
                    for (atomInData in allData) {
                        atomToRead.unify(atomInData, randomVarsScope)
                    }
                }
                val scanDurationNanos = System.nanoTime() - scanStartedAt

                if (iMeasure >= nMeasuresAsWarmup) {
                    measures[iMeasure - nMeasuresAsWarmup] = Pair(indexDurationNanos, scanDurationNanos)
                }
            }

            // ASSERT
            val averageIndexDurationNanos = measures.filterNotNull().map { it.first }.average()
            val averageScanDurationNanos = measures.filterNotNull().map { it.second }.average()

            val speedRatio = averageScanDurationNanos / averageIndexDurationNanos

            assert(speedRatio >= (allData.size.toDouble() / nReadsPerMeasure.toDouble()) * 0.7, { "btree index lookups are too slow for ${allData.size} data items; was only $speedRatio times faster than a table scan" })
            assert(speedRatio <= (allData.size.toDouble() / nReadsPerMeasure.toDouble()) * 2, { "btree index lookups for ${allData.size} data items are way faster than expected in the test (speed ratio = $speedRatio) - adjust test expectation?"})
        }.config(tags = setOf(LongRunning, Performance))

        "reading should be constant time" {
            // SETUP
            val index = AtomIndex()
            val allData = Array(50000, { Atom(randomString()) })
            for (i in 0 .. allData.lastIndex) {
                index.onInserted(allData[i], i)
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
                    index.onInserted(Atom(randomString()), nItem++)
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