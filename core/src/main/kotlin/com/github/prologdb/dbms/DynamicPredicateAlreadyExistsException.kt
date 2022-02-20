package com.github.prologdb.dbms

import com.github.prologdb.runtime.FullyQualifiedClauseIndicator
import com.github.prologdb.runtime.PrologException

class DynamicPredicateAlreadyExistsException(val fqi: FullyQualifiedClauseIndicator) : PrologException("The dynamic predicate $fqi already exists")