package com.github.prologdb.dbms

import com.github.prologdb.util.metadata.MetadataRepository

/**
 * Metadata about a knowledge base in general (does not include
 * nested metadata objects, such as for a predicate store).
 *
 * Wrapper for typesafe access to a [MetadataRepository]
 */
class KnowledgeBaseMetadata(private val nested: MetadataRepository) {

}