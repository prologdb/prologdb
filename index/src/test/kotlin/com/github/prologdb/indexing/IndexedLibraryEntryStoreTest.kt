package com.github.prologdb.indexing

import com.github.prologdb.runtime.knowledge.library.LibraryEntry
import com.github.prologdb.runtime.knowledge.library.PredicatePrototype
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import io.kotlintest.matchers.shouldEqual
import io.kotlintest.mock.`when`
import io.kotlintest.mock.mock
import io.kotlintest.specs.FreeSpec
import org.mockito.Mockito.verify

class IndexedLibraryEntryStoreTest : FreeSpec() {
    override val oneInstancePerTest = true

    init {
        "uses factory to obtain partial store for new predicates" {
            // SETUP
            val store = mock<IndexedPartialLibraryEntryStore>()
            val factory = object : IndexedPartialLibraryEntryStoreFactory {
                override fun createEntryStoreFor(prototype: PredicatePrototype): IndexedPartialLibraryEntryStore = store
            }
            val predicate = Predicate("name", arrayOf(Atom("argument")))
            val subject = IndexedLibraryEntryStore(factory)

            // ACT
            subject.add(predicate)

            // VERIFY
            verify(store).add(predicate)
        }

        "findFor does scan if no indexes exist" {
            // SETUP
            val predicate = mock<Predicate>()
            val allKnowledge: List<LibraryEntry> = listOf(
                mock<Predicate>(),
                mock<Predicate>(),
                mock<Predicate>(),
                mock<Predicate>()
            )

            val store = object : IndexedPartialLibraryEntryStore {
                override val prototype: PredicatePrototype = predicate
                override val exports: List<LibraryEntry> = allKnowledge

                override fun getIndexForArgument(argumentIndex: Int): PredicateArgumentIndex? = null

                override fun add(entry: LibraryEntry) {}

                override fun retract(unifiesWith: Predicate): LazySequence<Unification> = LazySequence.empty()

                override fun retractFact(fact: Predicate): LazySequence<Unification> = LazySequence.empty()
            }

            val factory = object : IndexedPartialLibraryEntryStoreFactory {
                override fun createEntryStoreFor(prototype: PredicatePrototype): IndexedPartialLibraryEntryStore = store
            }

            // ACT
            val subject = IndexedLibraryEntryStore(factory)
            subject.add(predicate)
            val result = subject.findFor(predicate)

            // ASSERT
            val resultList = result.toList()
            resultList shouldEqual allKnowledge
        }

        "findFor uses the indexes" {
            // SETUP
            val firstArgument = Atom("argument1")
            val secondArgument = Atom("argument2")
            val predicate = Predicate("p", arrayOf(firstArgument, secondArgument))

            val firstArgIndexSet = mock<IndexSet>()
            val secondArgIndexSet = mock<IndexSet>()
            val unionIndexSet = mock<IndexSet>()
            `when`(firstArgIndexSet.union(secondArgIndexSet)).thenReturn(unionIndexSet)
            `when`(secondArgIndexSet.union(firstArgIndexSet)).thenReturn(unionIndexSet)

            val firstArgumentIndex = mock<PredicateArgumentIndex>()
            val secondArgumentIndex = mock<PredicateArgumentIndex>()
            `when`(firstArgumentIndex.find(firstArgument)).thenReturn(firstArgIndexSet)
            `when`(secondArgumentIndex.find(secondArgument)).thenReturn(secondArgIndexSet)

            val allKnowledge: List<LibraryEntry> = listOf(
                mock<Predicate>(),
                mock<Predicate>(),
                mock<Predicate>(),
                mock<Predicate>()
            )

            val store = object : IndexedPartialLibraryEntryStore {
                override val prototype: PredicatePrototype = predicate
                override val exports: List<LibraryEntry> = allKnowledge

                override fun getIndexForArgument(index: Int): PredicateArgumentIndex? = if (index == 0) firstArgumentIndex else secondArgumentIndex

                override fun add(entry: LibraryEntry) {}

                override fun retract(unifiesWith: Predicate): LazySequence<Unification> = LazySequence.empty()

                override fun retractFact(fact: Predicate): LazySequence<Unification> = LazySequence.empty()
            }

            val factory = object : IndexedPartialLibraryEntryStoreFactory {
                override fun createEntryStoreFor(prototype: PredicatePrototype): IndexedPartialLibraryEntryStore = store
            }

            `when`(unionIndexSet.iterator()).thenReturn(IntRange(0, 2).iterator())

            // ACT
            val subject = IndexedLibraryEntryStore(factory)
            subject.add(predicate)
            val result = subject.findFor(predicate)

            // ASSERT
            val resultList = result.toList()
            resultList.size shouldEqual 3
            resultList[0] shouldEqual allKnowledge[0]
            resultList[1] shouldEqual allKnowledge[1]
            resultList[2] shouldEqual allKnowledge[2]
        }
    }
}