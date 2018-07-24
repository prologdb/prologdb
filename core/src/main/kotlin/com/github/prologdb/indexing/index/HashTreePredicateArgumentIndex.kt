package com.github.prologdb.indexing.index

import com.github.prologdb.indexing.PersistenceIDSet
import com.github.prologdb.indexing.IndexingException
import com.github.prologdb.indexing.ArrayPersistenceIDSet
import com.github.prologdb.indexing.PredicateArgumentIndex
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.storage.predicate.PersistenceID
import kotlin.reflect.KClass

@ConstantTimeRead
@ConstantTimeWrite
abstract class HashTreePredicateArgumentIndex<Value : Term, Element>(private val valueTypeClass: KClass<Value>) : PredicateArgumentIndex {
    /**
     * The root node; if null, the index is empty
     */
    private var rootNode: Node? = null

    /**
     * Used to lock the entire rootNode (instead of locking onto the node itself, works for null values, too)
     */
    private val rootNodeLock = Any()

    /**
     * @return The maximum number of levels the given value can be separated into, e.g. for a string `"foobar"` that
     * can be split at every character, this method should return `6`.
     */
    protected abstract fun getNumberOfElementsIn(value: Value): Int

    /**
     * @return the element to be used at the `index`th level of the b-tree;
     * @throws IndexOutOfBoundsException if the index exceeds the length of the value.
     */
    protected abstract fun getElementAt(value: Value, index: Int): Element

    /**
     * Used to build the individual nesting stages of the b-tree.
     * @return A hash of the given element; used in very similar fashion as [HashMap] uses [Any.hashCode].
     */
    protected abstract fun hashElement(element: Element): Int

    /**
     * @return Whether the two given elements equal
     */
    protected abstract fun elementsEqual(a: Element, b: Element): Boolean

    override fun find(argument: Term): PersistenceIDSet {
        if (rootNode == null) return PersistenceIDSet.NONE

        if (!valueTypeClass.isInstance(argument)) {
            throw IllegalArgumentException("The given argument is not an instance of ${valueTypeClass.qualifiedName}")
        }

        return find(argument as Value, rootNode!!, 0, getNumberOfElementsIn(argument) - 1)
    }

    /**
     * @param argument The argument to look up; is trusted to be of type ValueType
     * @param node The node to look within; is trusted to contain only compatible types
     * @param maxNestingLevel [getNumberOfElementsIn]`(argument) - 1`; is passed around as a value to avoid re-calculating
     */
    private fun find(argument: Value, node: Node, nestingLevel: Int, maxNestingLevel: Int): PersistenceIDSet {
        val element = getElementAt(argument, nestingLevel) ?: throw IndexingException("Index handled its data improperly", IndexOutOfBoundsException())

        if (nestingLevel == maxNestingLevel) {
            val targetNode = node.children[element]

            return if (targetNode == null) PersistenceIDSet.NONE else ArrayPersistenceIDSet(targetNode.persistenceIDs)
        }

        val childNode = node.children[element] ?: return PersistenceIDSet.NONE

        return find(argument, childNode, nestingLevel + 1, maxNestingLevel)
    }

    override fun onInserted(argumentValue: Term, persistenceID: PersistenceID) {
        if (!valueTypeClass.isInstance(argumentValue)) {
            throw IllegalArgumentException("This index can only hold values of type ${valueTypeClass.qualifiedName}")
        }
        argumentValue as Value

        synchronized(rootNodeLock) {
            if (rootNode == null) {
                rootNode = Node()
            }

            insert(argumentValue, persistenceID, rootNode!!, 0, getNumberOfElementsIn(argumentValue) - 1)
        }
    }

    /**
     * Like [find]; inserts the term where needed
     */
    private fun insert(argumentValue: Value, persistenceID: PersistenceID, node: Node, nestingLevel: Int, maxNestingLevel: Int) {
        val element = getElementAt(argumentValue, nestingLevel)

        synchronized(node) {
            if (nestingLevel == maxNestingLevel) {
                var targetNode = node.children[element]
                if (targetNode == null) {
                    targetNode = Node()
                    node.children[element] = targetNode
                }

                targetNode.persistenceIDs.add(persistenceID)
                return
            }
            else
            {
                var branch = node.children[element]
                if (branch == null) {
                    branch = Node()
                    node.children[element] = branch
                }

                insert(argumentValue, persistenceID, branch, nestingLevel + 1, maxNestingLevel)
            }
        }
    }

    override fun onRemoved(argumentValue: Term, fromPersistenceID: PersistenceID) {
        if (!valueTypeClass.isInstance(argumentValue)) {
            return
        }

        argumentValue as Value

        synchronized(rootNodeLock) {
            if (rootNode != null) {
                remove(argumentValue, fromPersistenceID, rootNode!!, 0, getNumberOfElementsIn(argumentValue) - 1)
            }
        }
    }

    private fun remove(argumentValue: Value, fromPersistenceID: PersistenceID, node: Node, nestingLevel: Int, maxNestingLevel: Int) {
        val element = getElementAt(argumentValue, nestingLevel) ?: throw IndexingException("Index handled its data improperly", IndexOutOfBoundsException())

        if (nestingLevel == maxNestingLevel) {
            val targetNode = node.children[element]
            if (targetNode != null) {
                targetNode.persistenceIDs.remove(fromPersistenceID)
            }

            return
        }

        val childNode = node.children[element] ?: return

        remove(argumentValue, fromPersistenceID, childNode, nestingLevel + 1, maxNestingLevel)
    }

    /**
     * A node in the tree; variables are public for easier persistence. Use the read&write methods anyway.
     */
    private inner class Node {

        /** Child/Branch nodes  */
        val children: HashMap<Element,Node> = HashMap(4)

        /**
         * The [PersistenceID]s where elements can be found whose last element was a key of this' nodes
         * parents [children] map.
         */
        val persistenceIDs: MutableList<PersistenceID> = ArrayList(4)
    }
}