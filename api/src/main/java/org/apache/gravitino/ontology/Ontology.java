/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.ontology;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;

/**
 * A metalake-level ontology: an Apache Ossie ontology document that declares the concepts of a
 * business and the relationships between them, managed by Gravitino.
 *
 * <p>An ontology is a machine-readable graph, not a glossary. A concept has a type from the closed
 * {@link ConceptType} set and relationships with cardinality, but no definition, synonyms, owner,
 * approval state or data bindings; those belong to the glossary's term object, which can reference
 * the concepts here. An ontology holds no assets and grants no access to data.
 *
 * <p>Ontologies are addressed by name alone within a metalake. A new ontology is in {@link
 * Status#DRAFT} status and is returned only to the callers who can manage it until it is published.
 *
 * <p>An {@code Ontology} is a snapshot. {@link OntologyOperations} returns new snapshots as the
 * ontology changes and does not modify this one.
 */
@Evolving
public interface Ontology extends Auditable {

  /**
   * The property key for the Apache Ossie document version.
   *
   * <p>For native ontology creation, Gravitino persists {@link
   * org.apache.gravitino.semantic.OssieVersion#DEFAULT_VERSION} when this property is omitted and
   * preserves explicitly supplied non-blank values. The persisted value is retained if the server
   * default changes later.
   */
  String PROPERTY_OSSIE_VERSION = "ossie-version";

  /** The status of an ontology. */
  enum Status {
    /**
     * The ontology is returned only to the callers who can manage it. It is omitted from lists and
     * getting it fails as if it did not exist.
     */
    DRAFT,

    /** The ontology is returned to every caller who can use it. */
    PUBLISHED
  }

  /**
   * Returns the name of the ontology.
   *
   * @return The ontology name.
   */
  String name();

  /**
   * Returns the comment of the ontology. It is the {@code description} of the exported Apache Ossie
   * document.
   *
   * @return The comment, or {@code null} if it is not set.
   */
  @Nullable
  String comment();

  /**
   * Returns the Gravitino-specific properties of the ontology. Implementations must return an
   * immutable map or a defensive copy.
   *
   * @return The properties, or an empty map if none are set.
   */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }

  /**
   * Returns the status of the ontology.
   *
   * @return The status.
   */
  Status status();

  /**
   * Returns the immutable concept graph of the ontology.
   *
   * @return The ontology definition.
   */
  OntologyDefinition definition();

  /**
   * Returns the audit information of the ontology.
   *
   * @return The audit information.
   */
  @Override
  Audit auditInfo();
}
