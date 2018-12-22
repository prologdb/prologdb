package com.github.prologdb.indexing

/**
 * Definition/requirements to an index as given in the meta knowledge base,
 * e.g.
 *     
 *     assert(index(
 *         foo_pk,
 *         foo(PK, _, _, _),
 *         [persistent, constant_time_read],
 *         []
 *     ))
 */
data class IndexDefinition(
    /** Name of the index, unique per [ClauseIndicator] of the template fact */
    val name: String,
    val template: IndexingTemplate,
    val requiredFeatures: Set<IndexFeature>,
    val optionalFeatures: Set<IndexFeature>
    // TODO sort directions??
)