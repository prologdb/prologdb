package com.github.prologdb.indexing

import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.VariableMapping
import com.github.prologdb.runtime.query.PredicateQuery
import com.github.prologdb.runtime.term.*
import com.github.prologdb.runtime.unification.VariableBucket
import java.util.*
import kotlin.reflect.KClass

typealias IndexKey = VariableBucket
fun IndexKey.indexKeyToString(): String {
    return values.joinToString(", ") { (variable, value) -> "$variable = ${value ?: Variable.ANONYMOUS}" }
}

/**
 * PrologDB indexes by defining a template fact. The template fact has the same name and arity
 * as the fact it is to help to index. The template fact can be unified with stored facts and
 * [PredicateInvocationQuery]s. When retaining only the template's variables from that unification, the
 * result is an [IndexKey]. [FactIndex]es can then associate those keys with [PersistenceID]s.
 */
class IndexingTemplate(
    val templateFact: Predicate,
    typeRestrictions: Map<Variable, KClass<out Term>>   
) {
    init {
        // type restrictions must relate to the template fact
        val unrelatedRestrictions = typeRestrictions.keys.filter { it !in templateFact.variables }.toSet()
        if (unrelatedRestrictions.isNotEmpty()) {
            throw IllegalArgumentException("The following variables have a type restriction but do not appear in the template fact: $unrelatedRestrictions")
        }
    }
    
    val typeRestrictions: Map<Variable, KClass<out Term>> = Collections.unmodifiableMap(HashMap(typeRestrictions))

    /**
     * @return the corresponding index key if the given fact can be stored / used for lookup, null otherwise.
     */
    fun unify(with: Predicate, randomVariableScope: RandomVariableScope = RandomVariableScope()): IndexKey? {
        val templateMapping = VariableMapping()
        val withRandom = randomVariableScope.withRandomVariables(with, VariableMapping())
        val templateWithRandom = randomVariableScope.withRandomVariables(templateFact, templateMapping)
        
        val randomUnification = withRandom.unify(templateWithRandom) ?: return null
        
        val resolvedVars = randomUnification.variableValues.withVariablesResolvedFrom(templateMapping)
        resolvedVars.retainAll(templateFact.variables)
        
        return if (isValid(resolvedVars)) resolvedVars else null 
    }

    /**
     * Throws an exception unless the given key
     * * has a variable bound for each of the variables in [templateFact]
     * * all the terms bound to these variables in [key] are ground.
     * 
     * @throws InvalidIndexKeyException If the given key is not valid for this template (could not have
     * been obtained from [unify]).
     */
    @Throws(InvalidIndexKeyException::class)
    fun refuseNonMatchingKey(key: IndexKey) {
        if (!isValid(key)) {
            throw InvalidIndexKeyException("Key ${key.indexKeyToString()} is not valid for index template $templateFact")
        }
    }

    /**
     * Checks whether the given key
     * * has a variable bound for each of the variables in [templateFact]
     * * all the terms bound to these variables in [key] are ground.
     * 
     * @return whether the given key is not valid for this template (could not have been obtained from
     * [unify]).
     */
    fun isValid(key: IndexKey): Boolean {
        if (templateFact.variables.any { !key.isInstantiated(it) }) {
            // not all variables from the template could be matched
            return false
        }

        if (templateFact.variables.any { !key[it].isGround }) {
            // not all bound values are ground
            return false
        }
        
        if (typeRestrictions.any { (variable, requiredType) -> !requiredType.isInstance(key[variable]) }) {
            // at least one type restriction is not matched
            return false
        }
        
        return true
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is IndexingTemplate) return false

        if (templateFact != other.templateFact) return false
        if (typeRestrictions != other.typeRestrictions) return false

        return true
    }

    override fun hashCode(): Int {
        var result = templateFact.hashCode()
        result = 31 * result + typeRestrictions.hashCode()
        return result
    }

    override fun toString(): String {
        var str = templateFact.toString()
        
        if (typeRestrictions.isNotEmpty()) {
            str += typeRestrictions.asIterable().joinToString(
                prefix = ", ",
                separator = ", ",
                transform = { (variable, typeClass) ->
                    when (typeClass) {
                        Atom::class          -> "atom($variable)"
                        PrologInteger::class -> "integer($variable)"
                        PrologDecimal::class -> "decimal($variable)"
                        PrologNumber::class  -> "number($variable)"
                        String::class        -> "string($variable)"
                        PrologList::class    -> "is_list($variable)"
                        else                 -> "$variable is_a ${typeClass.simpleName}"
                    }
                }
            )
        }
        
        return str
    }
    
    // TODO: implement intrinsic serialization/deserialization to avoid any issues with inconsistent implementations in index impls
}