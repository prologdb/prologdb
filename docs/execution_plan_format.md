# Query execution plans

prologdb can explain how it runs queries just like a lot of relational databases
do. To get the execution plan of a query, wrap it into `pdb_explain/1`:

    ?- pdb_explain(foo(X), bar(a, Y), Y > 5; z(Y)).
    
The result will have exactly one solution with exactly one random variable.
That variable is instantiated to a a predicate that models the execution plan,
e.g. something like this:

    _G1 = union(
        join(
            scan(foo(X)),
            lookup(
                bar(a, _G2),
                _G2 = Y,
                range(gt(5))
            )
        ),
        scan(z(Y))                    
    ).
    
-----  

Beyond the simple prove search there are two very basic building blocks
in prolog queries - and so are in the execution plan:
conjunction and disjunction. In queries, these are `,/2` and `;/2`, respectively.
In the execution plan, these show up as `join/_` and `union/_`:

    % without any indexes
    
    ?- pdb_explain(foo(X), bar(X)).
    _G1 = join(scan(foo(X)), scan(bar(X)))
    
    ?- pdb_explain(foo(X); bar(X)).
    _G1 = union(scan(foo(X)), scan(bar(X)))
    
The arguments to the `join` and `union` predicates an be anything, also nested
`join` and `union`. What follows is a list of the other possible predicates
and the querying strategy they represent:

## `scan/1`

Reads every known instance of the predicate from the data store and attempts
to unify. This is closely to a table scan in RDBMs.

### Example

`scan(foo(a))` will read every stored instance of `foo/1` and unify with
`foo(a)`.

## `deduce_from/1`

Runs all the rules known for the given predicate. These are essentially
sub-queries.

### Example

`deduce_from(foo(a))` will try to run all rules with the indicator `foo/1` against
the goal `foo(a)`; of course, as with regular prolog, rule heads that do not unify
with `foo(a)` will not be run.
    
## `lookup/3`

Utilizes an index to read only a subset of all known predicate instances. Rules
are not part of that lookup and are run separately.

The first argument is the predicate as it was given in the original query with
one change: the argument that is used for the lookup is replaced by a random
variable.

The second argument makes the replacement transparent. Is an instance of `=/2`
where the first argument is the random variable and the second argument is the
term it replaces.

The third argument is the strategy being used to look things up in the index (see
below).

E.g. a query `bar(a, Y), Y > 5` with an execution plan

    lookup(
        bar(a, _G6),
        _G6 = Y,
        range(gt(5))
    )
    
will do this:

1. use the index on the second argument of `bar/2` with the constraint
`range(gt(5))` (see below) to obtain all instances of `bar/2` where
the second argument is a number and is greater than 5.
2. unify each of the found predicates with `bar(a, Y)`
   

### Index lookup types

#### `unifies(:Term)`

All entries from the index are used where the indexed term unifies
with `:Term`. This does not promise any optimizations compared to a
separate `scan/1` step; but optimizations may be applied where the
database implementation sees fit.

#### `kind(:Kind)`

All entries from the indexed where the indexed term is of the specified `:Kind`. This
is used when an indexed predicate appears in conjunction with a type check on
an uninstantiated, indexed argument to that predicate.

##### Kinds

|regular prolog type check|index lookup type|
|-------------------------|-----------------|
|`integer/1`              |`kind(integer)`  |
|`float/1`                |`kind(float)`    |
|`number/1`               |`kind(number)`   |
|`string/1`               |`kind(string)`   |
|`atom/1`                 |`kind(atom)`     |
|`is_list/1`              |`kind(list)`     |
|`is_dict/1`              |`kind(dict)`     |
|`nonvar/1`               |not available    |
|`blob/2`                 |not available    |
|`atomic/1`               |not available    |
|`compound/1`             |not available    |


##### Examples

Query `bar(X), atom(X)`. Here, the first argument to `bar/1` is indexed,
the variable is definitely not instantiated at the time `bar/1` is called
and there is a conjunctive type check on that same variable. This gets 
optimized into this execution plan:

    lookup(
        bar(X),
        X,
        kind(atom)
    )

#### `range(:RangeQuery)`

This is used when the index supports range queries (e.g. a B-Tree). In
order for the query optimizer to use range queries, it must be able to
combine a conjunction between a prove search and a comparison (see 
examples).

The variants of `:RangeQuery`:

|Variant        |Description|
|---------------|-----------|
|`gt(:Value)`   |The indexed term is strictly greater than `:Value`|
|`gte(:Value)`  |The indexed term is greater than or equal to `:Value`|
|`lt(:Value)`   |The indexed term is strictly less than `:Value`|
|`lte(:Value)`  |The indexed term is less than or equal to `:Value`|
|`between(:Lower, :Upper, :LowerInclusive, :UpperInclusive)`|The indexed term is greater than `:Lower` and less than `:Upper`.  Whether indexed terms equal to `:Lower` and `:Upper` are also considered depends on `:LowerInclusive` and `:UpperInclusive`. These are either `1` or `0` representing inclusion or exclusion, respectively.|
  
### Examples

    % for foo/2, the first argument has an index
    ?- pdb_explain(foo(a, X)).
    _G1 = lookup(
        foo(_G1, X),
        _G1,
        unified(a)
    ) 
---    
    
    % for foo/2, the first argument has an index supporting range queries
    ?- pdb_explain(foo(X, Y), X > 10).
    _G1 = lookup(
        foo(_G1, Y),
        _G1,
        range(gt(10))
    )
    
    ?- pdb_explain(foo(X, Y), X > 10, X =< 50).
    _G1 = lookup(
        foo(_G1, Y),
        _G1,
        range(between(10, 50, 0, 1))
    )
---
    