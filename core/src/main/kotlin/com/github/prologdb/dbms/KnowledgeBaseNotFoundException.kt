package com.github.prologdb.dbms

import com.github.prologdb.runtime.PrologRuntimeException

class KnowledgeBaseNotFoundException(val name: String) : PrologRuntimeException("A knowledge base with name $name does not exist")