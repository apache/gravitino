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
package org.apache.gravitino.catalog;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieDocumentConverter;
import org.apache.gravitino.semantic.OssieDocumentConverter.ParsedSemanticModel;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;

/** Dispatches schema-scoped Semantic Model operations through the server pipeline. */
public interface SemanticModelDispatcher {

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
   * Imports an Ossie document after validating its sources under the current caller.
   *
   * <p>The document is parsed once, and source validation runs before any model write. Keeping the
   * validator caller-supplied avoids a dependency on server authorization in Core. Callers must
   * validate source existence even when authorization is disabled.
   *
   * @param namespace The destination schema namespace.
   * @param document The document to import.
   * @param sourceValidator The required caller-specific source validator.
   * @return The imported Semantic Model.
   */
  default SemanticModel importOssieDocument(
      Namespace namespace,
      OssieDocument document,
      Consumer<SemanticModelDefinition> sourceValidator) {
    Objects.requireNonNull(sourceValidator, "sourceValidator must not be null");
    ParsedSemanticModel model = OssieDocumentConverter.importDocument(document);
    sourceValidator.accept(model.definition());
    return importSemanticModel(
        NameIdentifier.of(namespace, model.name()),
        model.comment(),
        model.definition(),
        model.properties());
  }

  /**
   * Dispatches a parsed import through normalization and import-specific hooks.
   *
   * <p>This internal entry point is used by {@link #importOssieDocument(Namespace, OssieDocument,
   * Consumer)} after parsing and source validation. Each decorator implements it separately from
   * create; only the operation dispatcher reuses the underlying create implementation. The
   * top-level event dispatcher rejects direct calls to this internal entry point.
   *
   * @param ident The identifier of the model to import.
   * @param comment The model comment, or {@code null} if it has no comment.
   * @param definition The parsed native definition whose sources have been validated.
   * @param properties The Gravitino-specific model properties.
   * @return The imported Semantic Model.
   * @throws UnsupportedOperationException If called directly on the top-level event dispatcher.
   */
  SemanticModel importSemanticModel(
      NameIdentifier ident,
      @Nullable String comment,
      SemanticModelDefinition definition,
      Map<String, String> properties);

  /**
   * Exports a Semantic Model as a standalone Apache Ossie document.
   *
   * <p>Each decorator implements this operation separately from load; only the operation dispatcher
   * reuses the underlying load implementation before serializing the model.
   *
   * @param ident The Semantic Model identifier.
   * @param format The requested serialization format.
   * @return The serialized Ossie document.
   * @throws NoSuchSemanticModelException If the Semantic Model does not exist.
   * @throws IllegalSemanticModelException If the model cannot be represented as an Ossie document.
   */
  OssieDocument exportOssieDocument(NameIdentifier ident, OssieFormat format);

  /**
   * Applies changes atomically to a Semantic Model.
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
