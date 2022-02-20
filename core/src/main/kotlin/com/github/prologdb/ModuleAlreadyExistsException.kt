package com.github.prologdb

import com.github.prologdb.runtime.PrologException

class ModuleAlreadyExistsException(val name: String, message: String? = null, cause: Throwable? = null) : PrologException(
    message ?: "A module with name $name already exists",
    cause
)
