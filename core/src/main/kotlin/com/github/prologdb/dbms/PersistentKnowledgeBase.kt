package com.github.prologdb.dbms

import com.github.prologdb.execplan.planner.ExecutionPlanner
import java.nio.file.Path

/**
 * The central element of the database management system (compare RDBMS). For every
 * knowledge base a server is managing there is one instance of this class.
 */
class PersistentKnowledgeBase(
    val dataDirectory: Path,
    @Volatile var planner: ExecutionPlanner
) {

}