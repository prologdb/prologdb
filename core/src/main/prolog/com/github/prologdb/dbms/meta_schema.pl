:- module(schema).

:- use_module(essential($clauses), []).

:- native create_knowledge_base/1.
:- native drop_knowledge_base/1.
:- native knowledge_base/1.

assert(knowledge_base(Name)) :- create_knowledge_base(Name).
retract(knowledge_base(Name)) :- drop_knowledge_base(Name).

:- native create_module/2.
:- native drop_module/2.
:- native module/2.

assert(module(KnowledgeBaseName, ModuleName)) :- create_module(KnowledgeBaseName, ModuleName).
retract(module(KnowledgeBaseName, ModuleName)) :- drop_module(KnowledgeBaseName, ModuleName).