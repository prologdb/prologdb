:- use_module(essential($clauses), []).

default_dynamic_predicate_options([require(persistent)]).
default_index_options([require(persistent)]).

:- native create_dynamic_predicate/2.
:- native create_index/5.
:- module_transparent(assert/1).

assert(Module:dynamic(Indicator, Options)) :- create_dynamic_predicate(Module:Indicator, Options).

assert(Module:dynamic(Indicator)) :-
    default_dynamic_predicate_options(Options),
    assert(Module:dynamic(Indicator, Options))
    .

assert(dynamic(Indicator, Options)) :-
    current_module(Module),
    assert(Module:dynamic(Indicator, Options))
    .

assert(dynamic(Indicator)) :-
    default_dynamic_predicate_options(Options),
    assert(dynamic(Indicator, Options))
    .

assert(index(
    Name,
    GoalTemplate,
    Config,
    Options
)) :-
    current_module(Module),
    create_index(Name, Module, GoalTemplate, Config, Options)
    .

assert(index(
    Name,
    GoalTemplate,
    Config
)) :-
    default_index_options(Options),
    assert(index(Name, GoalTemplate, Config, Options))
    .
