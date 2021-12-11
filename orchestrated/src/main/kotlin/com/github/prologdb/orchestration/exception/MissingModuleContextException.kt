package com.github.prologdb.orchestration.exception

import com.github.prologdb.runtime.PrologRuntimeException

class MissingModuleContextException : PrologRuntimeException("No module selected. Select one using :- select_knowledge_base/1 and ?- module/1")