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
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.IllegalOntologyException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchOntologyException;
import org.apache.gravitino.exceptions.NonEmptyOntologyException;
import org.apache.gravitino.exceptions.OntologyAlreadyExistsException;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;

/**
 * Interface for the ontology operations of a metalake.
 *
 * <p>An ontology in {@link Ontology.Status#DRAFT} status is returned only to the callers who can
 * manage it. For other callers it is omitted from lists, and getting it throws {@link
 * NoSuchOntologyException}.
 *
 * <p>Names are unique per metalake, case-insensitively, and follow the tag name rules: up to 128
 * characters, and {@code /} is allowed and URL-encoded in paths.
 */
@Evolving
public interface OntologyOperations {

  /**
   * Lists the names of the ontologies in the metalake that are visible to the caller.
   *
   * @return The ontology names.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  String[] listOntologies() throws NoSuchMetalakeException;

  /**
   * Lists the ontologies in the metalake that are visible to the caller, with details.
   *
   * @return The ontologies.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  Ontology[] listOntologiesInfo() throws NoSuchMetalakeException;

  /**
   * Gets an ontology by its name.
   *
   * @param name The ontology name.
   * @return The ontology.
   * @throws NoSuchOntologyException If the ontology does not exist or is not visible to the caller.
   */
  Ontology getOntology(String name) throws NoSuchOntologyException;

  /**
   * Creates an ontology in {@link Ontology.Status#DRAFT} status.
   *
   * @param name The ontology name.
   * @param comment The comment, or {@code null}.
   * @param properties The Gravitino-specific properties, or {@code null}.
   * @param definition The concept graph of the ontology.
   * @return The created ontology.
   * @throws OntologyAlreadyExistsException If an ontology with the same name already exists.
   * @throws IllegalOntologyException If the definition is not a valid ontology.
   * @throws IllegalArgumentException If the name is null or blank.
   */
  Ontology createOntology(
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      OntologyDefinition definition)
      throws OntologyAlreadyExistsException, IllegalOntologyException, IllegalArgumentException;

  /**
   * Alters an ontology. Renaming an ontology does not rename the concepts inside it, so references
   * to a concept by name are unaffected; references to the ontology by name are not.
   *
   * @param name The ontology name.
   * @param changes The changes to apply.
   * @return The altered ontology.
   * @throws NoSuchOntologyException If the ontology does not exist or is not visible to the caller.
   * @throws OntologyAlreadyExistsException If the ontology is renamed and an ontology with the new
   *     name already exists.
   * @throws IllegalOntologyException If a replacement definition is not a valid ontology.
   * @throws IllegalArgumentException If the changes cannot be applied to the ontology.
   */
  Ontology alterOntology(String name, OntologyChange... changes)
      throws NoSuchOntologyException, OntologyAlreadyExistsException, IllegalOntologyException,
          IllegalArgumentException;

  /**
   * Deletes an ontology.
   *
   * @param name The ontology name.
   * @return True if the ontology is deleted, false if it does not exist.
   * @throws NonEmptyOntologyException If a mapping or a term reference still points at the
   *     ontology.
   */
  boolean deleteOntology(String name) throws NonEmptyOntologyException;

  /**
   * Imports a standalone Apache Ossie ontology document as a new ontology in {@link
   * Ontology.Status#DRAFT} status.
   *
   * <p>The document supplies the ontology name, comment, definition and Ossie version. The root
   * property set of an ontology document differs from that of a Semantic Model document, so this is
   * a separate entry point rather than a flag on the Semantic Model import.
   *
   * @param document The standalone Ossie ontology document and its serialization format.
   * @return The created ontology.
   * @throws OntologyAlreadyExistsException If an ontology with the document's name already exists.
   * @throws IllegalOntologyException If the document is not a valid ontology document.
   */
  default Ontology importOssieDocument(OssieDocument document)
      throws OntologyAlreadyExistsException, IllegalOntologyException {
    throw new UnsupportedOperationException("Ossie ontology import is not supported");
  }

  /**
   * Exports an ontology as a standalone Apache Ossie ontology document.
   *
   * <p>The {@link Ontology#PROPERTY_OSSIE_VERSION} property supplies the document version. An
   * ontology document has no place for external references, so Gravitino-specific properties are
   * not part of the document and are lost on a round trip.
   *
   * @param name The ontology name.
   * @param format The serialization format.
   * @return The standalone Ossie ontology document.
   * @throws NoSuchOntologyException If the ontology does not exist or is not visible to the caller.
   * @throws IllegalOntologyException If the ontology cannot be exported as a valid document.
   */
  default OssieDocument exportOssieDocument(String name, OssieFormat format)
      throws NoSuchOntologyException, IllegalOntologyException {
    throw new UnsupportedOperationException("Ossie ontology export is not supported");
  }
}
