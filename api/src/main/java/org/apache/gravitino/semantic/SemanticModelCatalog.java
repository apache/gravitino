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
package org.apache.gravitino.semantic;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;

/** The public catalog API for managing Semantic Models in a schema. */
@Evolving
public interface SemanticModelCatalog {

  /**
   * Lists the Semantic Models in a schema namespace.
   *
   * @param namespace The schema namespace.
   * @return The identifiers of the Semantic Models in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  NameIdentifier[] listSemanticModels(Namespace namespace) throws NoSuchSchemaException;

  /**
   * Loads a Semantic Model by identifier.
   *
   * @param ident The Semantic Model identifier.
   * @return The loaded Semantic Model.
   * @throws NoSuchSemanticModelException If the Semantic Model does not exist.
   */
  SemanticModel loadSemanticModel(NameIdentifier ident) throws NoSuchSemanticModelException;

  /**
   * Returns whether a Semantic Model exists.
   *
   * @param ident The Semantic Model identifier.
   * @return {@code true} if the Semantic Model exists, otherwise {@code false}.
   */
  default boolean semanticModelExists(NameIdentifier ident) {
    try {
      loadSemanticModel(ident);
      return true;
    } catch (NoSuchSemanticModelException e) {
      return false;
    }
  }

  /**
   * Creates a Semantic Model in a schema.
   *
   * @param ident The Semantic Model identifier.
   * @param comment The Semantic Model comment, or {@code null} if it has no comment.
   * @param definition The complete Semantic Model definition.
   * @param properties The Gravitino-specific Semantic Model properties.
   * @return The created Semantic Model.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws SemanticModelAlreadyExistsException If the Semantic Model already exists.
   * @throws IllegalSemanticModelException If the Semantic Model definition is invalid.
   */
  SemanticModel createSemanticModel(
      NameIdentifier ident,
      @Nullable String comment,
      SemanticModelDefinition definition,
      Map<String, String> properties)
      throws NoSuchSchemaException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException;

  /**
   * Applies changes atomically to a Semantic Model.
   *
   * <p>If any change is rejected or the resulting Semantic Model is invalid, no change is applied.
   *
   * @param ident The Semantic Model identifier.
   * @param changes The changes to apply.
   * @return The altered Semantic Model.
   * @throws NoSuchSemanticModelException If the Semantic Model does not exist.
   * @throws SemanticModelAlreadyExistsException If a rename conflicts with an existing Semantic
   *     Model.
   * @throws IllegalSemanticModelException If a change or the resulting Semantic Model is invalid.
   */
  SemanticModel alterSemanticModel(NameIdentifier ident, SemanticModelChange... changes)
      throws NoSuchSemanticModelException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException;

  /**
   * Drops a Semantic Model.
   *
   * @param ident The Semantic Model identifier.
   * @return {@code true} if the Semantic Model was dropped, or {@code false} if it did not exist.
   */
  boolean dropSemanticModel(NameIdentifier ident);
}
