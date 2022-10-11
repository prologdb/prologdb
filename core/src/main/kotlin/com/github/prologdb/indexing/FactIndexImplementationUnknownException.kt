package com.github.prologdb.indexing

import com.github.prologdb.runtime.PrologException

class FactIndexImplementationUnknownException(val id: String) : PrologException("A fact index implementation for $id is not known/loaded.")