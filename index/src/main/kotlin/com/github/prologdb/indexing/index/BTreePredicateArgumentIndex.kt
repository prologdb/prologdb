package com.github.prologdb.indexing.index

import com.github.prologdb.indexing.IndexSet
import com.github.prologdb.indexing.IndexingException
import com.github.prologdb.indexing.ListIndexSet
import com.github.prologdb.indexing.PredicateArgumentIndex
import com.github.prologdb.runtime.term.Term
import kotlin.reflect.KClass

/**
 * See [http://www.geeksforgeeks.org/b-tree-set-1-insert-2/]
 */
abstract class BTreePredicateArgumentIndex<Value : Term>(private val valueClass: KClass<Value>, private val comparator: Comparator<in Value>, private val valuesPerNode: Int = 4) : PredicateArgumentIndex {

    private var rootNode = Node()

    private fun requireOfValueType(term: Term): Value {
        if (!valueClass.isInstance(term)) {
            throw IllegalArgumentException("This index handles values of type ${valueClass.qualifiedName} only - given value is not an instance of the type.")
        }

        return term as Value
    }

    override fun find(argument: Term): IndexSet {
        val list = rootNode.find(requireOfValueType(argument))
        return if (list == null) IndexSet.NONE else ListIndexSet(list)
    }

    override fun onInserted(argumentValue: Term, atIndex: Int) {
        rootNode.insert(requireOfValueType(argumentValue), atIndex)
        while (rootNode.parent != null) {
            rootNode = rootNode.parent!!
        }
    }

    override fun onRemoved(argumentValue: Term, fromIndex: Int) {
        rootNode.remove(requireOfValueType(argumentValue), fromIndex)
    }

    private inner class ElementWrapper(val value: Value) : Comparable<ElementWrapper> {
        var nodeWithLesserValues: Node? = null
        val tablePositions: MutableList<Int> = ArrayList(4)

        override fun compareTo(other: ElementWrapper) = comparator.compare(this.value, other.value)
    }

    private inner class Node {
        /**
         * Holds all the values together with a pointer to the node containing strictly lesser values and the table
         * positions where the value can be found.
         */
        private val values: Array<ElementWrapper?> = Array(valuesPerNode, { null })

        /** The number of non-null elements in [values]; maintained by the [insert] and [remove] methods */
        private var nValues = 0

        /**
         * Pointer to a node with strictly greater values than the last non-null entry in [values]
         */
        private var rightEdgeNode: Node? = null

        /**
         * The parent node; is only null for the root node
         */
        var parent: Node? = null

        fun insert(value: Value, payload: Int) {
            if (nValues == values.size) {
                split()
            }


        }

        /**
         * Split as defined on [http://www.geeksforgeeks.org/b-tree-set-1-insert-2/]
         */
        fun split() {
            val splitIndex = nValues / 2

            val newRightNode = Node()
            newRightNode.parent = this.parent
            newRightNode.rightEdgeNode = this.rightEdgeNode

            // this node will be the "left" node / the new node with lesser values than the split point
            // move the values greater than the split value to the new right node
            for (i in splitIndex + 1 .. nValues) {
                newRightNode.values[i - splitIndex - 1] = values[i]
                values[i] = null
            }
            newRightNode.nValues = nValues - splitIndex - 1

            // the new right node is done here; now set this node up to be the "left" one
            val splitWrapper = values[splitIndex]!!
            this.rightEdgeNode = splitWrapper.nodeWithLesserValues
            splitWrapper.nodeWithLesserValues = this
            values[splitIndex] = null
            nValues = splitIndex

            // finally, insert the splitWrapper into the parent
            if (parent == null) {
                parent = Node()
            }
            val parent = parent!!
            val searchResult = parent.binarySearch(splitWrapper.value)
            if (searchResult > 0) throw IndexingException("This BTree should never have been in this state")
            // make room for the splitWrapper
            val insertionIndex = - searchResult - 1
            System.arraycopy(parent.values, insertionIndex, parent.values, insertionIndex + 1, parent.nValues - insertionIndex)
            parent.values[insertionIndex] = splitWrapper
            parent.nValues++
        }

        fun remove(value: Value, payload: Int) {

        }

        fun find(value: Value): List<Int>? {

        }

        /**
         * Does a binary search on [values] assuming ascending order. Contract like [java.util.Arrays.binarySearch]
         * @return see [java.util.Arrays.binarySearch]
         */
        private fun binarySearch(keyValue: Value): Int {
            var low = 0
            var high = nValues
            while (low < high) {
                val mid = (low + high) ushr 1
                val cmp = comparator.compare(keyValue, values[mid]!!.value)

                if (cmp < 0) {
                    low = mid + 1
                }
                else if (cmp > 0) {
                    high = mid - 1
                }
                else {
                    return mid // key found
                }
            }

            return -(low + 1)  // key not found.
        }
    }
}