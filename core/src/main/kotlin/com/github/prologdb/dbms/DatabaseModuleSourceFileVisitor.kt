package com.github.prologdb.dbms

import com.github.prologdb.parser.ModuleDeclaration
import com.github.prologdb.parser.Reporting
import com.github.prologdb.parser.SemanticError
import com.github.prologdb.parser.parser.DefaultModuleSourceFileVisitor
import com.github.prologdb.parser.source.SourceLocation
import com.github.prologdb.runtime.ClauseIndicator

class DatabaseModuleSourceFileVisitor(val moduleName: String) : DefaultModuleSourceFileVisitor(ModuleDeclaration(moduleName)) {
    override val operators = CombinedOperatorRegistry()

    override fun visitDynamicDeclaration(
        clauseIndicator: ClauseIndicator,
        location: SourceLocation
    ): Collection<Reporting> {
        return setOf(SemanticError("Predicates cannot be declared dynamic via the source file. Run ?- assert(dynamic($clauseIndicator)) in the meta-knowledgebase.", location))
    }
}