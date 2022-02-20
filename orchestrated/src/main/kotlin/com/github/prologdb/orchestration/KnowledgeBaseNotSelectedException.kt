package com.github.prologdb.orchestration

import com.github.prologdb.runtime.PrologException

class KnowledgeBaseNotSelectedException : PrologException("No knowledge base selected. Select one using :- knowledge_base(Name).")