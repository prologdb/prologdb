# Query execution plans

The prologdb can be seen as a prolog runtime with persistent state. Contrary to what one might
think then, the language with which to program/instruct that runtime is **not** prolog. It is
the execution plan. When prologdb receives a prolog query, the execution planner first
converts the query into an execution plan. The plan is then executed.
This has a bunch of advantages:

* when asked to show the execution plan to a prolog query, the result is *guaranteed* to be
  correct (correct as in: "what actually happens", not as in "what is supposed to happen")
* the execution plan contains all the detail useful for optimizing performance so that the
  prolog queries do not need to concern themselves with expressing such detail (e.g. what index
  to use, ...)
* developers can send queries as execution-plans (rather than prolog) to gain a better understanding
  of the execution plans (ultimately making them better at optimizing indices and planner settings).  
  Taken to an extreme, one could also use this to write queries optimized in ways that the planner
  is just not capable of (or including implicit assumptions that the planner just cannot reasonably
  make).
  
This document defines the execution plan language. This document is authoritative when it comes
to interpreting execution plans (which also means prologdb has to interpret its own execution
plans in the way defined here).

## Background Information

Here is some background information to aid in understanding the atomics of the execution plan
language:

When prologdb stores a fact with `assert/1` the fact gets assigned a *persistence id*.
This ID is unique to the predicate, e.g. unique among all instances of `foo/2`.

When a stored fact qualifies to be indexed, the indexed data of the fact is written to
the index along with the persistence id of the original fact. When querying an index,
only persistence ids and the indexed data can be obtained. To get the entire instance,
the fact store has to be consulted using the persistence id.

This separation is important: when using multiple indices for one fact lookup the
results from querying the indices have to be combined. The execution plan shows these steps
and as such readers of execution plans must be aware of that mechanism.

Facts and rules are treated very separately: rules are either inlined into the query or are
a completely separate query apart from the query that calls them (compare SQL sub-query). 

# Execution Plan Language

