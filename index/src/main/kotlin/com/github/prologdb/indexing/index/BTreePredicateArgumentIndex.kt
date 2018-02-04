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
class BTreePredicateArgumentIndex<Value : Term>(private val valueClass: KClass<Value>, private val comparator: Comparator<in Value>, private val valuesPerNode: Int = 5) : PredicateArgumentIndex {

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

        /** The number of non-null elements in [values]; maintained by the methods [insert], [remove] and [split] */
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
                parent!!.insert(value, payload)
                return
            }

            var targetValuesIndex = binarySearch(value)
            if (targetValuesIndex >= 0) {
                values[targetValuesIndex]!!.tablePositions.add(payload)
            } else {
                targetValuesIndex = -(targetValuesIndex + 1) // is now the index of the first element in values greater than value
                if (targetValuesIndex == nValues) {
                    if (rightEdgeNode == null) {
                        val newWrapper = ElementWrapper(value)
                        newWrapper.tablePositions.add(payload)
                        values[targetValuesIndex] = newWrapper
                        nValues++
                    } else {
                        rightEdgeNode!!.insert(value, payload)
                    }
                } else {
                    val targetWrapper = values[targetValuesIndex]!!
                    if (targetWrapper.nodeWithLesserValues != null) {
                        targetWrapper.nodeWithLesserValues!!.insert(value, payload)
                    } else {
                        // move the wrappers to make room for the new value
                        System.arraycopy(values, targetValuesIndex, values, targetValuesIndex + 1, nValues - targetValuesIndex)
                        val newWrapper = ElementWrapper(value)
                        newWrapper.tablePositions.add(payload)
                        values[targetValuesIndex] = newWrapper
                    }
                }
            }
        }

        /**
         * Split as defined on [http://www.geeksforgeeks.org/b-tree-set-1-insert-2/].
         */
        fun split() {
            assert(nValues == valuesPerNode)
            assert(parent == null || parent!!.nValues < valuesPerNode)

            val splitIndex = nValues / 2 // index of the value that will move to the parent

            /*
             order of operations:
             1. prepare the new left & right nodes with their correct values
             2. move the split value to the parent
             3. link everything up properly
             */

            // New right Node
            val newRightNode = Node()
            newRightNode.rightEdgeNode = this.rightEdgeNode
            for (i in splitIndex + 1 until nValues) {
                newRightNode.values[i - splitIndex - 1] = values[i]
            }
            newRightNode.nValues = nValues - splitIndex - 1

            // New left node
            val newLeftNode = Node()
            for (i in 0 until splitIndex) {
                newLeftNode.values[i] = values[i]
            }
            newLeftNode.nValues = splitIndex

            // wrapper for the split value
            val oldSplitWrapper = values[splitIndex]!!

            // insert the splitWrapper into the parent
            if (parent == null) {
                parent = Node()
            }
            val parent = parent!!
            val newSplitWrapper = ElementWrapper(oldSplitWrapper.value) // to be inserted into the parent
            val parentSearchResult = parent.binarySearch(newSplitWrapper.value)
            if (parentSearchResult >= 0) throw IndexingException("This BTree should never have been in this state")
            val insertionPoint = -(parentSearchResult + 1)
            System.arraycopy(parent.values, insertionPoint, parent.values, insertionPoint + 1, parent.nValues - insertionPoint)
            parent.values[insertionPoint] = newSplitWrapper
            parent.nValues++

            // link everything up
            newLeftNode.rightEdgeNode = oldSplitWrapper.nodeWithLesserValues
            newLeftNode.parent = parent
            newSplitWrapper.nodeWithLesserValues = newLeftNode
            // find the place to link newRightNode to
            if (insertionPoint == parent.nValues - 1) {
                assert(parent.rightEdgeNode == null || parent.rightEdgeNode == this)
                parent.rightEdgeNode = newRightNode
            } else {
                val nextLargerElementInParentThanSplitValue = parent.values[insertionPoint + 1]!!
                assert(nextLargerElementInParentThanSplitValue.nodeWithLesserValues == this)
                parent.values[insertionPoint + 1]!!.nodeWithLesserValues = newRightNode
            }
            newRightNode.parent = parent
        }

        fun remove(value: Value, payload: Int) {
            TODO()
        }

        fun find(value: Value): List<Int>? {
            var wrapperIndex = binarySearch(value)
            if (wrapperIndex >= 0) {
                return values[wrapperIndex]!!.tablePositions
            } else {
                wrapperIndex = -(wrapperIndex + 1)
                if (wrapperIndex == nValues) {
                    return rightEdgeNode?.find(value)
                }
                val wrapper = values[wrapperIndex]!!
                return wrapper.nodeWithLesserValues?.find(value)
            }
        }

        /**
         * Does a binary search on [values] assuming ascending order. Contract like [java.util.Arrays.binarySearch]
         * @return see [java.util.Arrays.binarySearch]
         */
        private fun binarySearch(keyValue: Value): Int {
            var low = 0
            var high = nValues - 1

            while (low <= high) {
                val mid = (low + high).ushr(1)
                val midVal = values[mid]!!
                val cmp = comparator.compare(midVal.value, keyValue)
                if (cmp < 0) {
                    low = mid + 1
                } else if (cmp > 0) {
                    high = mid - 1
                } else {
                    return mid // key found
                }
            }

            return -(low + 1)  // key not found.
        }
    }
}