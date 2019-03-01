package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.LazySequence
import com.github.prologdb.dbms.PersistentKnowledgeBase
import com.github.prologdb.indexing.IndexDefinition
import com.github.prologdb.indexing.IndexFeature
import com.github.prologdb.indexing.IndexingTemplate
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.*
import com.github.prologdb.runtime.unification.Unification
import kotlin.reflect.KClass

/**
 * A prolog interface to metainformation stored for a [PersistentKnowledgeBase]:
 * 
 * * the existing indices
 * * user permissions
 */
class Metabase(
    private val actualKB: PersistentKnowledgeBase
) : ProgrammaticServerKnowledgeBase(ISOOpsOperatorRegistry, {
    callable("assert"/1) { session, args ->
        val fact = args[0] as? CompoundTerm
            ?: throw PrologRuntimeException("Cannot assert ${args[0].prologTypeName}s (only CompoundTerms)")

        when (fact.functor) {
            "index" -> {
                actualKB.createIndex(indexDefinitionFromProlog(fact))
                return@callable LazySequence.of(Unification.TRUE)
            }
            else -> throw PrologRuntimeException("CompoundTerm ${ClauseIndicator.of(fact)} is not dynamic.")
        }
    }
}) {
}

private fun indexTemplateFromProlog(term: Term): IndexingTemplate {
    val list = term.commaCompoundTermToList()
    val templateFact = list[0] as? CompoundTerm
        ?: throw PrologRuntimeException("Invalid indexing template: must be a comma-separated list of CompoundTerms.")
    
    val typeRestrictions: Map<Variable, KClass<out Term>> = if (list.size == 1) emptyMap() else {
        list.subList(1, list.size)
            .map(Term::toTypeRestriction)
            .toMap()
    }
    
    try {
        return IndexingTemplate(templateFact, typeRestrictions)
    } catch (ex: IllegalArgumentException) {
        throw PrologRuntimeException("Failed to construct an index template from $templateFact: ${ex.message}", ex)
    }
}

private fun Term.toTypeRestriction(): Pair<Variable, KClass<out Term>> {
    this as? CompoundTerm
        ?: throw PrologRuntimeException("$this is not a valid index type restriction: must be a CompoundTerm")
    if (this.arity != 1) throw PrologRuntimeException("$this is not a valid index type restriction: arity must be 1")
    
    val targetVar = this.arguments[0] as? Variable ?: PrologRuntimeException("$this is not a valid index type restriction: argument must be unbound")

    val restrictedType: KClass<out Term> = when (this.functor) {
        "atom"    -> Atom::class
        "integer" -> PrologInteger::class
        "decimal" -> PrologDecimal::class
        "number"  -> PrologNumber::class
        "string"  -> PrologString::class
        "is_list" -> PrologList::class
        else -> throw PrologRuntimeException("$this is not a valid index type restriction: ${this.functor} is not a recognized and supported type-check")
    }
    
    return Pair(targetVar as Variable, restrictedType)
}

private fun indexDefinitionFromProlog(term: CompoundTerm): IndexDefinition {
    if (term.arity != 4 || term.functor != "index") {
        throw PrologRuntimeException("Cannot construct an index definition from an instance of ${ClauseIndicator.of(term)}")
    }
    
    val nameTerm = term.arguments[0]
    val templateTerm = term.arguments[1]
    val requiredFeaturesTerm = term.arguments[2]
    val desiredFeaturesTerm = term.arguments[3]
    
    nameTerm as? Atom ?: throw PrologRuntimeException("First argument to index/4 must be an atom, got ${nameTerm.prologTypeName}")
    val template = try {
        indexTemplateFromProlog(templateTerm)
    }
    catch (ex: PrologRuntimeException) {
        throw PrologRuntimeException("Invalid indexing template (argument 2 to index/4): ${ex.message}", ex)
    }
    val requiredFeaturesList = requiredFeaturesTerm.asEnumValues<IndexFeature>()
    val requiredFeatures     = requiredFeaturesList.toSet()
    val desiredFeaturesList  = desiredFeaturesTerm.asEnumValues<IndexFeature>()
    val desiredFeatures      = desiredFeaturesList.toSet()
    
    if (requiredFeatures.size != requiredFeaturesList.size) {
        // required features not unique
        val duplicates = requiredFeaturesList
            .groupingBy { it }
            .eachCount()
            .filter { it.value > 1 }
            .map { it.key }
        
        throw PrologRuntimeException("Invalid index definition: required features (argument 3 to index/4) contains duplicates $duplicates")
    }
    
    if (desiredFeatures.size != desiredFeaturesList.size) {
        // desired features not unique
        val duplicates = requiredFeaturesList
            .groupingBy { it }
            .eachCount()
            .filter { it.value > 1 }
            .map { it.key }

        throw PrologRuntimeException("Invalid index definition: desired features (argument 4 to index/4) contains duplicates $duplicates")
    }
    
    try {
        return IndexDefinition(nameTerm.name, template, requiredFeatures, desiredFeatures)
    }
    catch (ex: IllegalArgumentException) {
        throw PrologRuntimeException("Failed to create an index definition from $term: ${ex.message}", ex)
    }
}

private fun Term.commaCompoundTermToList(): List<Term> {
    val list = mutableListOf<Term>()
    var pivot: Term = this
    while (pivot is CompoundTerm && pivot.arity == 2 && pivot.functor == ",") {
        list.add(pivot.arguments[0])
        pivot = pivot.arguments[1]
    }
    
    list.add(pivot)
    
    return list
}
