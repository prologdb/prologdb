package com.github.prologdb.transactions

import com.github.prologdb.runtime.knowledge.library.MutableLibraryEntryStore
import com.github.prologdb.runtime.knowledge.library.SimpleLibraryEntryStore
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.PredicateBuilder
import com.github.prologdb.runtime.term.Variable
import io.kotlintest.matchers.beEmpty
import io.kotlintest.matchers.should
import io.kotlintest.matchers.shouldEqual
import io.kotlintest.specs.FreeSpec

class TransactionalLibraryEntryStoreTest : FreeSpec() {
    override val oneInstancePerTest = true

init {

    val foo = PredicateBuilder("foo")
    val a = Atom("a")
    val b = Atom("b")
    val X = Variable("X")

    for (implementationFactory in implementationFactories) {
        val underlying = SimpleLibraryEntryStore()
        val subject = implementationFactory(underlying)

        "${subject.javaClass.simpleName}" - {
            "actions without transaction are applied directly" - {
                "add" {
                    // ACT
                    subject.add(foo(a))

                    // ASSERT
                    underlying.findFor(foo(a)).toList() shouldEqual listOf(foo(a))
                }

                "retract" {
                    // SETUP
                    subject.add(foo(a))
                    assert(underlying.findFor(foo(a)).first() == foo(a))

                    // ACT
                    subject.retract(foo(a)).tryAdvance()

                    // ASSERT
                    underlying.findFor(foo(a)).toList() should beEmpty()
                }
            }

            "actions within transaction are not applied" - {
                "add" {
                    // ACT
                    subject.beginTransaction()
                    subject.add(foo(a))

                    // ASSERT
                    underlying.findFor(foo(a)).toList() should beEmpty()
                }

                "retract" {
                    // SETUP
                    underlying.add(foo(a))
                    assert(underlying.findFor(foo(a)).first() == foo(a))

                    // ACT
                    subject.beginTransaction()
                    subject.retract(foo(a))

                    // ASSERT
                    underlying.findFor(foo(a)).toList() shouldEqual listOf(foo(a))
                }
            }

            "changes within a transaction are visible within the transactions" - {
                "add" {
                    // SETUP
                    assert(subject.findFor(foo(a)).toList().isEmpty())

                    // ACT
                    subject.beginTransaction()
                    subject.add(foo(a))

                    // ASSERT
                    subject.findFor(foo(a)).toList() shouldEqual listOf(foo(a))
                }

                "retract" {
                    // SETUP
                    underlying.add(foo(a))
                    underlying.add(foo(a))
                    underlying.findFor(foo(a)).toList() shouldEqual listOf(foo(a), foo(a))

                    // ACT
                    subject.beginTransaction()
                    subject.retract(foo(a)).tryAdvance()

                    // ASSERT
                    subject.findFor(foo(a)).toList() shouldEqual listOf(foo(a))
                }
            }

            "commit" - {
                "changes are applied" {
                    // SETUP
                    underlying.add(foo(a))

                    // ACT
                    subject.beginTransaction()
                    subject.add(foo(b))
                    subject.retract(foo(a)).tryAdvance()
                    subject.commit()

                    // ASSERT
                    underlying.findFor(foo(X)).toList() shouldEqual listOf(foo(b))
                }

                "changes are applied in the correct order" {
                    // SETUP
                    underlying.add(foo(a))
                    underlying.add(foo(a))
                    underlying.add(foo(b))

                    // ACT
                    subject.beginTransaction()
                    subject.retractAll(foo(X))
                    subject.add(foo(a))
                    subject.commit()

                    // ASSERT
                    underlying.findFor(foo(X)).toList() shouldEqual listOf(foo(a))
                }
            }

            "rollback" - {
                "changes rolled back are not applied" {
                    // SETUP
                    underlying.add(foo(a))

                    // ACT
                    subject.beginTransaction()
                    subject.add(foo(b))
                    subject.retract(foo(a)).tryAdvance()
                    subject.rollback()

                    // ASSERT
                    underlying.findFor(foo(X)).toList() shouldEqual listOf(foo(a))
                }
            }
        }
    }
}
    companion object {
        val implementationFactories = listOf<(MutableLibraryEntryStore) -> TransactionalLibraryEntryStore>(
            ::LayeredInMemoryTransactionalLibraryEntryStore
        )
    }
}