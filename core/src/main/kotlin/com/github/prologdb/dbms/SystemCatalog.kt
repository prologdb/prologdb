package com.github.prologdb.dbms

import com.fasterxml.jackson.annotation.JsonIgnore
import com.github.prologdb.runtime.HasFunctorAndArity
import java.util.UUID

data class SystemCatalog(
    @JsonIgnore
    val revision: Long,
    val knowledgeBases: Set<KnowledgeBase>
) {
    class KnowledgeBase(
        val name: String,
        val modules: Set<Module>
    ) {

    }

    data class Module(
        val name: String,
        val predicates: Set<Predicate>
    ) {

    }

    data class Predicate(
        override val functor: String,
        override val arity: Int,
        val uuid: UUID
    ) : HasFunctorAndArity {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Predicate

            if (uuid != other.uuid) return false

            return true
        }

        override fun hashCode(): Int {
            return uuid.hashCode()
        }
    }

    companion object {
        val INITIAL: SystemCatalog = SystemCatalog(0, emptySet())
    }
}