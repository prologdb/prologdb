:- use_module(essential($clauses), []).
:- use_module(meta(module_common)).

:- native(source/1).
:- native('dynamic'/2).

dynamic(Indicator) :- dynamic(Indicator, _).