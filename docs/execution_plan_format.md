# Query execution plans

## Background Information

When prologdb stores a predicate with `assert/1` the predicate gets assigned a *persistence id*.
This ID is unique to the indicator, e.g. unique among all instances of `foo/2` but may be duplicate
across indicators.

When a stored predicate qualifies to be indexed, the indexed data of the instance is written to
the index along with the persistence id of the original record. When querying an index,
only persistence ids and the indexed data can be obtained. To get the entire instance,
the fact store has to be consulted using the persistence id.

This separation is important: when using multiple indices for one fact lookup the
results from querying the indices have to be combined. The execution plan shows these steps
and as such readers of execution plans must be aware of that mechanism.

Facts and rules are treated very separately: rules are either inlined into the query or are
a completely separate query apart from the query that calls them (compare SQL sub-query). 


## Structure

Execution plans are formatted as prolog code, a special syntax similar to that of prolog
queries. `|` denotes a conjunction/join just like the `,` in prolog whereas `;` denotes
a combination/union (same as in prolog). The operands these act upon are different than in
prolog, though.

Each operand is a function invocation, denoted by a prolog predicate (e.g. `fact_scan(foo/3)`).
Functions MAY take an arbitrary number of arguments, MAY produce a sequence as output and MAY
take a value as their primary input. The `|` operator pipes two functions together: for each
element in the sequence returned by the left-hand-side function, the function on the right
hand side is invoked with the element as the primary input.  
For example: given this prolog query: `foo(X), bar(X, Y)`: `foo(X)` returns a sequence of
solutions, each instantiating X. For every solution, the next predicate is sort-of given that
solution as an input (so that the `X` in `bar(X, Y)` can be replaced with its instantiation
from the `foo(X)` query). The `bar(X, Y)` is then run, producing another sequence of solutions.  
The execution plan for `foo(X), bar(X, Y)` may look like so:

    fact_scan(foo/1) | unify(foo(X)) | fact_scan(bar(X, Y))
    
Here, `fact_scan` produces a sequence of all facts known for `foo/1`. Each of these facts is 
then given to `unify`, which takes the fact as the primary input and yields the unification
as an output (if any). 

Query: `foo(a)`  
Plan: `fact_scan(foo/1) | unify(foo(a))`

### `fact_scan(indicator) -> seq<[+, predicate]>`

### `[+, predicate]`