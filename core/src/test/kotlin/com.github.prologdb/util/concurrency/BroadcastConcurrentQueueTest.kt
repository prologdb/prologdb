package com.github.prologdb.util.concurrency

import io.kotlintest.forAll
import io.kotlintest.matchers.beTheSameInstanceAs
import io.kotlintest.matchers.should
import io.kotlintest.matchers.shouldEqual
import io.kotlintest.specs.FreeSpec

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
        forAll(setOf(consumerQueue1, consumerQueue2, consumerQueue3)) { consumerQueue ->
            consumerQueue.remove() shouldEqual "Foo"
            consumerQueue.remove() shouldEqual "Bar"
            consumerQueue.remove() shouldEqual "Fizz"
            consumerQueue.remove() shouldEqual "Buzz"
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
        consumerQueue1.remove() shouldEqual "Foo"
        consumerQueue1.remove() shouldEqual "Bar"
        consumerQueue1.remove() shouldEqual "Fizz"
        consumerQueue1.remove() shouldEqual "Buzz"

        consumerQueue2.remove() shouldEqual "Fizz"
        consumerQueue2.remove() shouldEqual "Buzz"
    }

    "double registration impossible" {
        // SETUP
        val queueHub = BroadcastConcurrentQueue<String>()

        // ACT
        queueHub.registerConsumer(1) should beTheSameInstanceAs(queueHub.registerConsumer(1))
    }
})