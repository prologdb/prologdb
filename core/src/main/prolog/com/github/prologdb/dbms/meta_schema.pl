:- module(schema).

:- use_module(essential($clauses), []).

:- native create_knowledge_base/1.

assert(knowledge_base(Name)) :- create_knowledge_base(Name).