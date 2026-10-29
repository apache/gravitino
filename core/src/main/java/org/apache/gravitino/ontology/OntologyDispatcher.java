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
import org.apache.gravitino.exceptions.IllegalOntologyException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchOntologyException;
import org.apache.gravitino.exceptions.NonEmptyOntologyException;
import org.apache.gravitino.exceptions.OntologyAlreadyExistsException;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;

/**
 * Dispatches ontology operations to the backing store.
 *
 * <p>Ontologies are metalake-scoped, so every operation names the metalake explicitly. This mirrors
 * {@link OntologyOperations} for the client (which binds the metalake) the way {@code
 * TagDispatcher} relates to {@code SupportsTags}.
 */
public interface OntologyDispatcher {

  /**
   * Lists the ontology names in a metalake.
   *
   * @param metalake The metalake name.
   * @return The ontology names.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  String[] listOntologies(String metalake) throws NoSuchMetalakeException;

  /**
   * Lists the ontologies in a metalake with their definitions.
   *
   * @param metalake The metalake name.
   * @return The ontologies.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  Ontology[] listOntologiesInfo(String metalake) throws NoSuchMetalakeException;

  /**
   * Gets an ontology by name.
   *
   * @param metalake The metalake name.
   * @param name The ontology name.
   * @return The ontology.
   * @throws NoSuchOntologyException If the ontology does not exist.
   */
  Ontology getOntology(String metalake, String name) throws NoSuchOntologyException;

  /**
   * Creates an ontology.
   *
   * @param metalake The metalake name.
   * @param name The ontology name.
   * @param comment The comment, or {@code null}.
   * @param properties The Gravitino-specific properties, or {@code null}.
   * @param definition The concept graph.
   * @return The created ontology.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws OntologyAlreadyExistsException If an ontology with the same name exists.
   * @throws IllegalOntologyException If the definition is not a valid ontology.
   */
  Ontology createOntology(
      String metalake,
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      OntologyDefinition definition)
      throws NoSuchMetalakeException, OntologyAlreadyExistsException, IllegalOntologyException;

  /**
   * Alters an ontology.
   *
   * @param metalake The metalake name.
   * @param name The ontology name.
   * @param changes The changes to apply.
   * @return The altered ontology.
   * @throws NoSuchOntologyException If the ontology does not exist.
   * @throws OntologyAlreadyExistsException If the ontology is renamed to an existing name.
   * @throws IllegalOntologyException If a replacement definition is not a valid ontology.
   */
  Ontology alterOntology(String metalake, String name, OntologyChange... changes)
      throws NoSuchOntologyException, OntologyAlreadyExistsException, IllegalOntologyException;

  /**
   * Deletes an ontology.
   *
   * @param metalake The metalake name.
   * @param name The ontology name.
   * @return {@code true} if the ontology was deleted, {@code false} if it did not exist.
   * @throws NonEmptyOntologyException If the ontology is still referenced by a mapping.
   */
  boolean deleteOntology(String metalake, String name) throws NonEmptyOntologyException;

  /**
   * Creates the ontology described by an Apache Ossie document.
   *
   * @param metalake The metalake name.
   * @param document The parsed Ossie document.
   * @return The created ontology.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws OntologyAlreadyExistsException If an ontology with the document's name exists.
   * @throws IllegalOntologyException If the document is not a valid ontology document.
   */
  Ontology importOssieDocument(String metalake, OssieDocument document)
      throws NoSuchMetalakeException, OntologyAlreadyExistsException, IllegalOntologyException;

  /**
   * Exports an ontology as an Apache Ossie document.
   *
   * @param metalake The metalake name.
   * @param name The ontology name.
   * @param format The document format.
   * @return The exported document.
   * @throws NoSuchOntologyException If the ontology does not exist.
   */
  OssieDocument exportOssieDocument(String metalake, String name, OssieFormat format)
      throws NoSuchOntologyException;

  /**
   * Returns the ontology entity, for authorization checks that need more than the public view.
   *
   * @param ident The ontology identifier.
   * @return The ontology entity.
   * @throws NoSuchEntityException If the ontology does not exist.
   */
  OntologyEntity getOntologyEntity(NameIdentifier ident) throws NoSuchEntityException;
}
