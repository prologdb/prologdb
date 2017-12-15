package com.github.prologdb.indexing.index

import com.github.prologdb.runtime.term.Atom
import io.kotlintest.matchers.beEmpty
import io.kotlintest.matchers.should
import io.kotlintest.matchers.shouldEqual
import io.kotlintest.matchers.shouldNot
import io.kotlintest.specs.FreeSpec

class BTreeWithHashMapPredicateArgumentIndexTest : FreeSpec() {
    override val oneInstancePerTest = true
init {
    val index = AtomIndex()

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

        "index correction on insertion before" {
            index.onInserted(Atom("foo"), 0)
            index.onInserted(Atom("bar"), 1)
            index.onInserted(Atom("peter"), 1)

            val indexesForBar = index.find(Atom("bar")).toSet()
            val indexesForPeter = index.find(Atom("peter")).toSet()

            indexesForBar shouldEqual setOf(2)
            indexesForPeter shouldEqual setOf(1)
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

        "index correction on remove" {
            index.onInserted(Atom("bar"), 1222)
            index.onInserted(Atom("foo"), 5222)

            index.find(Atom("bar")).toSet() shouldEqual setOf(1222)
            index.find(Atom("foo")).toSet() shouldEqual setOf(5222)

            index.onRemoved(Atom("bar"), 1222)

            index.find(Atom("foo")).toSet() shouldEqual setOf(5221)
        }
    }
}}

private class AtomIndex : BTreeWithHashMapPredicateArgumentIndex<Atom, Char>(Atom::class) {
    override fun getNumberOfElementsIn(value: Atom): Int = value.name.length

    override fun getElementAt(value: Atom, index: Int): Char = value.name[index]

    override fun hashElement(element: Char): Int = element.hashCode()

    override fun elementsEqual(a: Char, b: Char): Boolean = a == b
}