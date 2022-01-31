# Controlling Persistence (equivalent to DDL)

```prolog
%-- USE ...
:- knowledge_base($meta).

%-- CREATE DATABASE
?- assert(knowledge_base(fooBase)).
/*
outside of transaction, atomic via filesystem
*/

%-- DROP DATABASE
?- retract(knowledge_base(fooBase)).

/*
suspended until end of transaction by setting removedBy=txid 
*/

%-- ALTER DATABASE ... RENAME TO
:- rename_knowledge_base(fooBase, fooBase2, [retract_alias]). 

:- use_knowledge_base(meta(fooBase)).

:- module(foo). %-- to avoid the module prefixes

%-- fetch source code of a module as a prolog string (compare stored procedures and views)
?- foo:source(Output).

%-- update source code of a moudle
?- foo:source("This is the new prolog source").

%-- CREATE TABLE
?- assert(dynamic(foo:persistent/1)).

%-- DROP TABLE
?- retract(dynamic(foo:persistent/1)).

%-- ALTER TABLE ... RENAME TO
:- rename_predicate(foo:persistent/1, persistent2).

%-- ALTER TABLE ... ADD CONSTRAINT (TYPE / CHECK)
?- assert(constraint(constraint_name, (foo:persistent(Term), (atom(Term) ; integer(Term), Term > 2)))).

%-- ALTER TABLE ... ADD UNIQUE CONSTRAINT
?- assert(constraint(constraint_name, (foo:persistent(Term), once(foo:persistent(Term))))).

%-- ALTER TALBE ... ADD CONSTRAINT FOREIGN KEY m:n / 1:n
?- assert(constraint(constraint_name, (foo:persistent(Term), other(_, Term)))).

%-- ALTER TABLE ... ADD CONSTRAINT FOREIGN KEY m:1 / 1:1
?- assert(constraint(constraint_name, (foo:persistent(Term), once(other(_, Term))))).

%-- ALTER TABLE ... DROP CONSTRAINT
?- retract(constraint(constraint_name, (foo:persistent(_), _))).

%-- CREATE INDEX (all integers in the second argument to foo:persistent/3)
?- assert(index(
    index_name,
    (foo:persistent(_, Term, _), integer(Term)),
    [efficient_range_lookup]
)).
%-- other examples
?- assert(index(
    index_name,
    (foo:persistent(_, bar(A), B)),
    [efficient_range_lookup, orderBy(desc(A), asc(B))]
)).
%-- indexes only on PrimaryKey but stores Other along with the index entries
?- assert(index(
    index_name,
    (foo:persistent(PrimaryKey, _, Other)),
    [efficient_single_lookup, key(PrimaryKey)]
)).

%-- DROP INDEX
?- retract(index(index_name, (foo:persistent(_, _, _), _), _)).
```