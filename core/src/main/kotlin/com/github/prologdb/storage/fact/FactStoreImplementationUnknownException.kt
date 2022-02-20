package com.github.prologdb.storage.fact

import com.github.prologdb.runtime.PrologException

class FactStoreImplementationUnknownException(val id: String) : PrologException("A fact store implementation for $id is not known/loaded.")