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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.OntologyInUseException;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;

/** Manages ontologies in a metalake. */
@Evolving
public interface SupportsOntologies {
  /**
   * @return The names of the visible ontologies.
   */
  String[] listOntologies();

  /**
   * Loads the current ontology.
   *
   * @param name The ontology name.
   * @return The current ontology.
   */
  Ontology loadOntology(String name);

  /**
   * Creates a new ontology.
   *
   * @param name The new ontology name.
   * @param comment The optional description.
   * @param definition The ontology definition.
   * @param properties The Gravitino properties.
   * @return The created ontology.
   */
  Ontology createOntology(
      String name,
      @Nullable String comment,
      OntologyDefinition definition,
      Map<String, String> properties);

  /**
   * Imports an Ossie document and creates an ontology.
   *
   * @param document The Ossie document.
   * @return The imported ontology.
   */
  Ontology importOntology(OssieDocument document);

  /**
   * Exports the current ontology as an Ossie document.
   *
   * @param name The ontology name.
   * @param format The requested document format.
   * @return The Ossie document.
   */
  OssieDocument exportOntology(String name, OssieFormat format);

  /**
   * Applies changes to an ontology in order and atomically.
   *
   * @param name The ontology name.
   * @param changes The ordered changes.
   * @return The updated ontology.
   * @throws OntologyInUseException If the replacement definition would invalidate a live mapping
   *     that references the ontology. None of the changes are applied in this case.
   */
  Ontology alterOntology(String name, OntologyChange... changes);

  /**
   * Drops an ontology.
   *
   * @param name The ontology name.
   * @return {@code true} if the ontology was dropped, or {@code false} if it did not exist.
   * @throws OntologyInUseException If any live mapping references the ontology.
   */
  boolean dropOntology(String name);

  /**
   * Lists the visible mappings of an ontology.
   *
   * @param name The ontology name.
   * @return Mapping identifiers relative to the metalake.
   */
  NameIdentifier[] listOntologyMappings(String name);
}
