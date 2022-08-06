package com.github.prologdb.util.concurrency


import io.kotest.core.spec.style.FreeSpec
import io.kotest.inspectors.forAll
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beTheSameInstanceAs

class BroadcastConcurrentQueueTest : FreeSpec({
    "all consumers get all items after registration" {
        // SETUP
        val queueHub = BroadcastConcurrentQueue<String>()

        // ACT
        val consumerQueue1 = queueHub.registerConsumer(1)
        val consumerQueue2 = queueHub.registerConsumer(2)
        val consumerQueue3 = queueHub.registerConsumer(3)

        queueHub.add("Foo")
        queueHub.add("Bar")
        queueHub.add("Fizz")
        queueHub.add("Buzz")

        // ASSERT
        setOf(consumerQueue1, consumerQueue2, consumerQueue3).forAll { consumerQueue ->
            consumerQueue.remove() shouldBe "Foo"
            consumerQueue.remove() shouldBe "Bar"
            consumerQueue.remove() shouldBe "Fizz"
            consumerQueue.remove() shouldBe "Buzz"
        }
    }

    "consumers get only items added after registration" {
        // SETUP
        val queueHub = BroadcastConcurrentQueue<String>()

        // ACT
        val consumerQueue1 = queueHub.registerConsumer(1)

        queueHub.add("Foo")
        queueHub.add("Bar")

        val consumerQueue2 = queueHub.registerConsumer(2)

        queueHub.add("Fizz")
        queueHub.add("Buzz")

        // ASSERT
        consumerQueue1.remove() shouldBe "Foo"
        consumerQueue1.remove() shouldBe "Bar"
        consumerQueue1.remove() shouldBe "Fizz"
        consumerQueue1.remove() shouldBe "Buzz"

        consumerQueue2.remove() shouldBe "Fizz"
        consumerQueue2.remove() shouldBe "Buzz"
    }

    "double registration impossible" {
        // SETUP
        val queueHub = BroadcastConcurrentQueue<String>()

        // ACT
        queueHub.registerConsumer(1) should beTheSameInstanceAs(queueHub.registerConsumer(1))
    }
})