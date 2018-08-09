package com.github.prologdb.util.memory

import io.kotlintest.matchers.beGreaterThanOrEqualTo
import io.kotlintest.matchers.should
import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.FreeSpec

class FirstFitHeapManagerTest : FreeSpec({
    "allocate" - {
        "exact size when minimum viable = 1" {
            val heap = FirstFitHeapManager(1024, 1, 1f)

            val allocated = heap.allocate(1000, false)

            allocated!!
            allocated.last - allocated.first shouldBe 999L
            heap.freeSpaceAmount shouldBe 24L
        }

        "allocate all space exact match" {
            val heap = FirstFitHeapManager(1024, 1, 1f)

            val allocatedA = heap.allocate(24, false)
            val allocatedB = heap.allocate(1000, false)

            allocatedA!!
            allocatedB!!
            allocatedA.last - allocatedA.first shouldBe 23L
            allocatedB.last - allocatedB.first shouldBe 999L
            heap.freeSpaceAmount shouldBe 0L
            heap.allocate(1, false) shouldBe null
        }

        "splits of at least minimum viable size" {
            val heap = FirstFitHeapManager(1024, 100, 1f)

            val allocatedA = heap.allocate(950, false)
            allocatedA!!
            heap.freeSpaceAmount shouldBe 0L
            heap.allocate(50, false) shouldBe null
        }

        "not enlarged if not allowed" {
            val heap = FirstFitHeapManager(1024, 100, 1f)

            heap.allocate(512, false)
            heap.size shouldBe 1024L
            heap.allocate(1024, false) shouldBe null
            heap.size shouldBe 1024L
            heap.freeSpaceAmount shouldBe 512L
        }

        "not enlarged if not necessary but allowed" {
            val heap = FirstFitHeapManager(1024, 10, 1f)

            val allocated = heap.allocate(1000, true)

            allocated!!
            heap.size shouldBe 1024L
            heap.freeSpaceAmount shouldBe 24L
        }

        "enlarged if necessary and allowed" {
            val heap = FirstFitHeapManager(1024, 1, 1f)

            val allocated = heap.allocate(2048, true)

            allocated!!
            allocated.last - allocated.first shouldBe 2047L
            heap.size should beGreaterThanOrEqualTo(2048L)
        }
    }

    "free" - {
        "space available after free" {
            val heap = FirstFitHeapManager(1024, 1, 0f)

            val allocated = heap.allocate(900, false)
            allocated!!
            heap.freeSpaceAmount shouldBe (1024L - 900L)
            heap.free(allocated)
            heap.freeSpaceAmount shouldBe 1024L
        }

        "without defragmentation" - {
            "not enough space without defrag - not allocated" {
                val heap = FirstFitHeapManager(1024, 1, 0f)

                val allocated = heap.allocate(768L, false)
                allocated!!
                heap.free(allocated)

                heap.allocate(980, false) shouldBe null
            }

            "not enough space without defrag - enlarged" {
                val heap = FirstFitHeapManager(1024, 1, 0f)

                val allocatedA = heap.allocate(768L, false)
                allocatedA!!
                heap.free(allocatedA)

                val allocatedB = heap.allocate(980, true)
                allocatedB!!
                allocatedB.last - allocatedB.first shouldBe 979L
                heap.size should beGreaterThanOrEqualTo(768L + 980L)
            }
        }

        "with defragmentation" - {
            "enough space only with defrag" {
                val heap = FirstFitHeapManager(1024, 128, 1f)

                val allocatedA = heap.allocate(980, false)
                allocatedA!!
                heap.free(allocatedA)
                val allocatedB = heap.allocate(512, false)
                val allocatedC = heap.allocate(512, false)

                allocatedB!!
                allocatedC!!
                allocatedB.last - allocatedB.first shouldBe 511L
                allocatedC.last - allocatedC.first shouldBe 511L
                heap.size shouldBe 1024L
                heap.freeSpaceAmount shouldBe 0L
            }
        }
    }

    "builder" - {
        "without spec all allocated" {
            val builder = FirstFitHeapManager.fromExistingLayoutSubtractiveBuilder(1, 0f)
            val heap = builder.build(1024)

            heap.freeSpaceAmount shouldBe 0L
            heap.allocate(10, false) shouldBe null
        }

        "free space can be allocated" {
            val builder = FirstFitHeapManager.fromExistingLayoutSubtractiveBuilder(1, 0f)
            builder.markAreaFree(0L..63L)
            builder.markAreaFree(103L..120L)
            val heap = builder.build(1024)

            val allocatedA = heap.allocate(50, false)
            val allocatedB = heap.allocate(17, false)

            allocatedA!!
            allocatedA.last - allocatedA.first should beGreaterThanOrEqualTo(49L)

            allocatedB!!
            allocatedB.last - allocatedB.first should beGreaterThanOrEqualTo(17L)
        }

        "overlaps are handled gracefully" {
            val builder = FirstFitHeapManager.fromExistingLayoutSubtractiveBuilder(1, 0f)

            builder.markAreaFree(15L..70L)
            builder.markAreaFree(40L..98L)
            val heap = builder.build(100)

            heap.freeSpaceAmount shouldBe 84L
        }
    }
})