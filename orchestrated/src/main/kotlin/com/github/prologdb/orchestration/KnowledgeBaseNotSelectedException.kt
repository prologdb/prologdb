package com.github.prologdb.orchestration

import com.github.prologdb.runtime.PrologRuntimeException

class KnowledgeBaseNotSelectedException : PrologRuntimeException("No knowledge base selected. Select one using :- knowledge_base(Name).")