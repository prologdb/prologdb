package com.github.prologdb.net.util

import com.github.prologdb.runtime.CircularTermException
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.VariableBucket

/**
 * Sorts entries in the given bucket such that, when applied in sequence even references between entries
 * in the bucket are resolved correctly. So e.g. given the bucket `B = 1, A = B` and the subject term `foo(A)`,
 * applying the bucket in original order would yield `foo(B)`. After resorting with this function to `A = B, B = 1`
 * the result would be `foo(1)`.
 *
 * *Example:*
 * ```kotlin
 * var _term = originalTerm
 * originalBucket.sortForSubstitution().forEach {
 *     _term = _term.substituteVariables(it.asSubstitutionMapper())
 * }
 * // _term is now correctly substituted
 * ```
 *
 * @return When successively applied using [Term.substituteVariables] all substitutions, including references, are done.
 *
 * @throws PrologRuntimeException If there are circular references in the bucket.
 */
fun VariableBucket.sortForSubstitution(): List<VariableBucket> {
    val variablesToSort = HashSet(this.variables)
    val bucket = this

    fun Variable.isReferenced(): Boolean {
        for (toSort in variablesToSort) {
            if (toSort === this) continue
            if (this in bucket[toSort].variables) {
                return true
            }
        }

        return false
    }

    val sorted = ArrayList<VariableBucket>(variablesToSort.size)

    while (variablesToSort.isNotEmpty()) {
        val free = variablesToSort.filterNot { it.isReferenced() }
        if (free.isEmpty()) {
            // there are variables left but none of them are free -> circular dependency!
            throw CircularTermException("Circular dependency in variable instantiations between $variablesToSort")
        }

        val subBucket = VariableBucket()
        for (freeVariable in free) {
            subBucket.instantiate(freeVariable, bucket[freeVariable])
        }
        sorted.add(subBucket)

        variablesToSort.removeAll(free)
    }

    return sorted
}