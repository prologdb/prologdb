:- use_module(essential($clauses), []).
:- use_module(essential($dynamic), [current_module/1]).

default_dynamic_predicate_options([require(persistent)]).
default_index_options([require(persistent)]).

:- native create_dynamic_predicate/2.
:- native create_index/5.
:- module_transparent(assert/1).

assert(Module:dynamic(Indicator, Options)) :- '$meta_module_common':create_dynamic_predicate(Module:Indicator, Options).

assert(Module:dynamic(Indicator)) :-
    '$meta_module_common':default_dynamic_predicate_options(Options),
    '$meta_module_common':assert(Module:dynamic(Indicator, Options))
    .

assert(dynamic(Indicator, Options)) :-
    '$dynamic':current_module(Module),
    '$meta_module_common':assert(Module:dynamic(Indicator, Options))
    .

assert(dynamic(Indicator)) :-
    '$meta_module_common':default_dynamic_predicate_options(Options),
    '$meta_module_common':assert(dynamic(Indicator, Options))
    .

assert(index(
    Name,
    GoalTemplate,
    Config,
    Options
)) :-
    '$dynamic':current_module(Module),
    '$meta_module_common':create_index(Name, Module, GoalTemplate, Config, Options)
    .

assert(index(
    Name,
    GoalTemplate,
    Config
)) :-
    '$meta_module_common':default_index_options(Options),
    '$meta_module_common':assert(index(Name, GoalTemplate, Config, Options))
    .
