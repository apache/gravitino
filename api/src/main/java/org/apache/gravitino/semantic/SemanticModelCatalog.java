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
   * Imports a standalone Apache Ossie document as a new Semantic Model in a schema.
   *
   * <p>The model name comes from the document. Import uses the same validation as {@link
   * #createSemanticModel}; it does not replace an existing model. The document is parsed according
   * to its declared format, without falling back to another format.
   *
   * <p>REST clients send {@link OssieDocument#content()} as the raw request body, without
   * JSON-encoding the document string again. The {@code Content-Type} header selects the parser:
   * {@code application/json} for JSON, or {@code application/yaml}, {@code application/x-yaml}, or
   * {@code text/yaml} for YAML. If the header is absent, the REST endpoint defaults to YAML.
   *
   * <p>A root {@link CustomExtension#GRAVITINO_PROPERTIES_VENDOR} extension is consumed as model
   * properties before creation. Its data must encode a JSON object with string values, and at most
   * one such extension is allowed. Other custom extensions are preserved.
   *
   * @param namespace The destination schema namespace.
   * @param document The standalone Ossie document and its serialization format.
   * @return The created Semantic Model.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws SemanticModelAlreadyExistsException If a model with the document's name already exists.
   * @throws IllegalSemanticModelException If the document or Semantic Model definition is invalid.
   * @throws UnsupportedOperationException If Ossie import is not supported.
   */
  default SemanticModel importOssieSemanticModel(Namespace namespace, OssieDocument document)
      throws NoSuchSchemaException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    throw new UnsupportedOperationException("Ossie import is not supported");
  }

  /**
   * Exports a Semantic Model as a standalone Apache Ossie document.
   *
   * <p>The {@link SemanticModel#PROPERTY_OSSIE_VERSION} property supplies the document version.
   * Other properties, if present, are serialized as a JSON object in the data of a root {@link
   * CustomExtension#GRAVITINO_PROPERTIES_VENDOR} extension.
   *
   * @param ident The Semantic Model identifier.
   * @param format The requested serialization format.
   * @return The serialized Ossie document and its format.
   * @throws NoSuchSemanticModelException If the Semantic Model does not exist.
   * @throws IllegalSemanticModelException If the model cannot be represented as an Ossie document.
   * @throws UnsupportedOperationException If Ossie export is not supported.
   */
  default OssieDocument exportOssieSemanticModel(NameIdentifier ident, OssieFormat format)
      throws NoSuchSemanticModelException, IllegalSemanticModelException {
    throw new UnsupportedOperationException("Ossie export is not supported");
  }

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
