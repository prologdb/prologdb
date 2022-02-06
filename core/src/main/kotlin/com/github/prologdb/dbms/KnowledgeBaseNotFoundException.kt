package com.github.prologdb.dbms

import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.Term

class KnowledgeBaseNotFoundException(val specifier: Term) : PrologRuntimeException("Knowledge base $specifier does not exist") {
    constructor(name: String) : this(Atom(name))
}