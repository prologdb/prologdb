package com.github.prologdb.dbms

import com.github.prologdb.execplan.planner.ExecutionPlanner

/**
 * The central element of the database management system (compare RDBMS). For every
 * prolog database a database server is managing there is one instance of this class.
 * For interactions with the database, obtain a [PrologDatabaseView].
 */
class PrologDatabaseManager(
    @Volatile var planner: ExecutionPlanner
) {

}

class PrologDatabaseWorker(
    private val view: PrologDatabaseView
) {

}