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

Execution plans are written with prolog syntax. Variable instantiation semantics across goals
also works 1:1. However, the operators are different (except `=/2` and `;/2`) and there are no
user-defined predicates.  
Secondly, execution plans borrow semantics from stream-based languages such as [bash pipes] or
[jq] (you'll see later).

## Functor

The core element of an execution plan are functors. A functor is written as a goal/predicate
and resembles a function invocation. A functor takes two inputs

* an input element
* arguments

It then does it's job. The result is a sequence of elements. The results are lazily computed
as each element of the result sequence is passed on to the next functor as the input or
treated as a solution to the query. *The solution contains only the variable instantiations
present in the last functor/stage, not the result element of the functor.*

For example, lets look at the `fact_scan` functor:

    * -> fact_scan(indicator) -> [fact, +]
    
This signature means "Takes anything as input (`*`), takes one argument which is a predicate
indicator and yields a sequence of facts along with their persistence id (`+`)."

In an actual execution plan, the functor could be used like so:

    fact_scan(bar/1)
    
Which would cause prologdb to read all facts of `bar/1` off the disk (and then do nothing with it).

To actually make use of that, we can combine it with the `unify` functor:

    [fact, +] -> unify(fact) -> +
    
It works pretty much like `=/2` does in prolog: the functor unifies the input fact and
the argument fact. If unification succeeds, it yields the input persistence id and instantiates
variables as necessary. Otherwise, it yields nothing.

So, both in combination:

    fact_scan(bar/1) | unify(bar(X))
    
This would read all instances of `bar/1` off disk and then unify each instance with `bar(X)`,
instantiating `X` as necessary. For every element `unify` yields, the server will recognize one
solution and send the variable instantiations (for `X`) out.

## Functor combination

You surely have noticed the `|` operator. It works like the `,` in regular prolog **plus** that
it chains the functors inputs and outputs together (for each element in the LHS, the RHS gets
invoked once).

Finally, the `;` operator has to be mentioned: it works just like in prolog. For example:

Query:
    
    foo(X) ; bar(X)
    
Execution plan:

    (fact_scan(foo/1) | unify(foo(X))) ; (fact_scan(bar/1) | unify(bar(X)))
    
## Signatures

This document describes all available functors. To describe those, signatures (like shown above)
are used. These have a simple type system associated with them:

Types available:

* `=` (Unification ; a set of variable-value pairs)
* `+` (Persistence ID)
* `fact` (A predicate instance, e.g. `bar(2)`)
* `atom` (A prolog atom, used for indentifiers)
* `*` any value, used only for functor input.
* `void` no value
* `[A, B, ...]` tuple of values of type A and B, e.g. `[fact, +]` is a tuple of a fact and a persistence ID
* `indicator` a predicate indicator, e.g. `bar/1`

## Functors

### `* -> fact_scan(indicator) -> [fact, +]`

**Input**: discards the input  
**Action**: Reads all facts of the given indicator from the underlying store (from disk, or, if available,
from cache).  
**Yields**: the facts along with their associated persistence ID.  
**Instantiates**: nothing.

This is the equivalent of an SQL table-scan

### `[fact, +] -> unify(fact) -> +`

**Action:**  Does an isolated unification of the input fact with the argument fact.
**Yields:** The input persistence ID if the unification succeeds.
**Instantiates:** the variables found in the argument fact

An isolated unification is different from a regular unification in that the variable scopes
are of LHS and RHS are separated, e.g.:

Using regular unification:

    ?- a(X, 1, Z) = a(1, 1, 1).
    X = 1,
    Z = 1.
    
    ?- a(X, 2) = a(1, X).
    false.
    
Using isolated unification:

    ?- a(X, 1, Z) = a(1, 1, 1).
    LHS: X = 1, Z = 1.
    RHS: true.
    
    ?- a(X, 2) = a(1, X).
    LHS: X = 1.
    RHS: X = 2.  

The `unify` functor instantiates the variables for the argument fact:

Input: `a(1, X)`  
Functor invocation: `unify(a(Z, 2))`
Instantiates: `Z = 1`

Regular prolog does an isolated unification when invoking a rule by randomizing
the variables, e.g.:

    a(1, X) :- X > 5.
    ?- a(X, 10)
    
`a(1, X) = a(X, 10)` does not even unify, not speaking of satisfying the `X > 5` constraint.
However, prolog first randomizes the variables in both the goal and rule before going ahead:

1. The rule becomes `a(1, _G1) :- _G1 > 5.`
2. The goal becomes `a(_G2, 10)`
3. Prolog does `a(_G2, 10) = a(1, _G1)` instantiating `_G2 = 1, _G1 = 10`
4. then carries on running the rule query with the instantiated variables...
5. The final result is `X = 1`

Step 1 to 3 are an isolated unification.

[bash pipes]: https://ryanstutorials.net/linuxtutorial/piping.php
[jq]: https://stedolan.github.io/jq/tutorial/ 