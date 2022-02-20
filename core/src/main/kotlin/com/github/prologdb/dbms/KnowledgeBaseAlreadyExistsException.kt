package com.github.prologdb.dbms

import com.github.prologdb.runtime.PrologException
import com.github.prologdb.runtime.term.Term

class KnowledgeBaseAlreadyExistsException(val specifier: Term) : PrologException("A knowledge base with the name $specifier already exists")