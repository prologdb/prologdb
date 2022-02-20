package com.github.prologdb.dbms

import com.github.prologdb.runtime.PrologException
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.Term

class KnowledgeBaseNotFoundException(val specifier: Term) : PrologException("Knowledge base $specifier does not exist") {
    constructor(name: String) : this(Atom(name))
}