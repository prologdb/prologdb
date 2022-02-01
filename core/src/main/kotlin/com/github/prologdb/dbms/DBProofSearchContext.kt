package com.github.prologdb.dbms

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.async.Principal
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.runtime.Clause
import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.FullyQualifiedClauseIndicator
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.module.ModuleReference
import com.github.prologdb.runtime.proofsearch.Authorization
import com.github.prologdb.runtime.proofsearch.PrologCallable
import com.github.prologdb.runtime.proofsearch.ProofSearchContext
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.runtime.util.OperatorRegistry
import com.github.prologdb.storage.fact.FactStore

interface DBProofSearchContext : ProofSearchContext {
    val database: PrologDatabase
    val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase
    val moduleName: String

    fun getFactStore(predicate: SystemCatalog.Predicate): FactStore

    override fun deriveForModuleContext(moduleName: String): DBProofSearchContext
}

class DBProofSearchContextImpl(
    override val database: PrologDatabase,
    override val knowledgeBaseCatalog: SystemCatalog.KnowledgeBase,
    override val moduleName: String,
    private val runtimeEnvironment: PhysicalDatabaseRuntimeEnvironment,
    private val lookupTable: Map<ClauseIndicator, Pair<ModuleReference, PrologCallable>>,
    override val principal: Principal,
    override val authorization: Authorization,
    override val randomVariableScope: RandomVariableScope
) : DBProofSearchContext {
    private val selfModule = runtimeEnvironment.loadedModules.getValue(moduleName)
    override val fulfillAttach: suspend LazySequenceBuilder<Unification>.(Query, initialVariables: VariableBucket) -> Unification? = { q, variables ->
        val executionPlan = runtimeEnvironment.database.executionPlanner.planExecution(q, this@DBProofSearchContextImpl, randomVariableScope)
        yieldAllFinal(
            executionPlan
                .invoke(this@DBProofSearchContextImpl, LazySequence.of(Pair(variables, Unit)))
                .mapRemaining { (variables, _) -> Unification(variables) }
        )
    }

    override val operators: OperatorRegistry = selfModule.localOperators

    override fun resolveCallable(simpleIndicator: ClauseIndicator): Pair<FullyQualifiedClauseIndicator, PrologCallable>? {
        selfModule.allDeclaredPredicates[simpleIndicator]?.let { callable ->
            val fqIndicator = FullyQualifiedClauseIndicator(moduleName, simpleIndicator)
            return Pair(fqIndicator, callable)
        }

        lookupTable[simpleIndicator]?.let { (sourceModule, callable) ->
            val fqIndicator = FullyQualifiedClauseIndicator(sourceModule.moduleName, ClauseIndicator.of(callable))

            return Pair(fqIndicator, callable)
        }

        return null
    }

    override fun resolveModuleScopedCallable(goal: Clause): Triple<FullyQualifiedClauseIndicator, PrologCallable, Array<out Term>>? {
        if (goal.functor != ":" || goal.arity != 2 || goal !is CompoundTerm) {
            return null
        }

        val moduleNameTerm = goal.arguments[0]
        val unscopedGoal = goal.arguments[1]

        if (moduleNameTerm !is Atom || unscopedGoal !is CompoundTerm) {
            return null
        }

        val simpleIndicator = ClauseIndicator.of(unscopedGoal)

        if (moduleNameTerm.name == this.moduleName) {
            val callable = selfModule.allDeclaredPredicates[simpleIndicator]
                ?: throw PrologRuntimeException("Predicate $simpleIndicator not defined in context of module ${this.moduleName}")

            val fqIndicator = FullyQualifiedClauseIndicator(this.moduleName, simpleIndicator)
            return Triple(fqIndicator, callable, unscopedGoal.arguments)
        }

        val module = runtimeEnvironment.loadedModules[moduleNameTerm.name]
            ?: throw PrologRuntimeException("Module ${moduleNameTerm.name} not loaded")

        val callable = module.exportedPredicates[simpleIndicator]
            ?: if (simpleIndicator in module.allDeclaredPredicates) {
                throw PrologRuntimeException("Predicate $simpleIndicator not exported by module ${module.name}")
            } else {
                throw PrologRuntimeException("Predicate $simpleIndicator not defined by module ${module.name}")
            }

        return Triple(
            FullyQualifiedClauseIndicator(module.name, simpleIndicator),
            callable,
            unscopedGoal.arguments
        )
    }

    override fun deriveForModuleContext(moduleName: String): DBProofSearchContext {
        return runtimeEnvironment.deriveProofSearchContextForModule(this, moduleName)
    }

    override fun getFactStore(predicate: SystemCatalog.Predicate): FactStore {
        return database.getFactStore(predicate.uuid)
    }
}