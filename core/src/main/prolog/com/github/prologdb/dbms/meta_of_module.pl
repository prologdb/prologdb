:- use_module(essential($clauses), []).
:- use_module(meta(module_common)).
:- use_module(meta($meta_module_common)).

:- native(source/1).
:- native('dynamic'/2).
:- native(index/4).

dynamic(Indicator) :- dynamic(Indicator, _).