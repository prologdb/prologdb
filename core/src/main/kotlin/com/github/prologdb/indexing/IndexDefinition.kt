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
) {
    init {
        // none of the optional features may be a required feature
        optionalFeatures.firstOrNull { it in requiredFeatures }?.let {
            throw IllegalArgumentException("Optional feature $it is also listed as a required feature.")
        }
    }
}