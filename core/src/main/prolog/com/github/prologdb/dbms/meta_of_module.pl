:- use_module(essential($clauses), []).
:- use_module(meta(module_common)).
:- use_module(essential($meta_module_common)).

:- native(source/1).
:- native('dynamic'/2).
:- native(index/4).

dynamic(Indicator) :- dynamic(Indicator, _).
index(Name, GoalTemplate, Config) :- index(Name, GoalTemplate, Config, _).