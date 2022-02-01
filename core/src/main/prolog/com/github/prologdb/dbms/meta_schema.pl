:- module(schema).

:- use_module(essential($clauses), []).

:- native create_knowledge_base/1.
:- native drop_knowledge_base/1.

assert(knowledge_base(Name)) :- create_knowledge_base(Name).
retract(knowledge_base(Name)) :- drop_knowledge_base(Name).