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

import java.util.Objects;
import java.util.function.Consumer;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieDocumentConverter;
import org.apache.gravitino.semantic.OssieDocumentConverter.ImportedSemanticModel;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelCatalog;
import org.apache.gravitino.semantic.SemanticModelDefinition;

/** A dispatcher specialization for schema-scoped Semantic Model operations. */
public interface SemanticModelDispatcher extends SemanticModelCatalog {

  /**
   * {@inheritDoc}
   *
   * <p>Uses the import pipeline without caller-specific source validation. Server callers must use
   * the overload accepting a source validator, even when authorization is disabled.
   */
  @Override
  default SemanticModel importOssieDocument(Namespace namespace, OssieDocument document) {
    return importOssieDocument(namespace, document, definition -> {});
  }

  /**
   * Imports an Ossie document after validating its sources under the current caller.
   *
   * <p>The document is parsed once, and source validation runs before any model write. Keeping the
   * validator caller-supplied avoids a dependency on server authorization in Core.
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
    ImportedSemanticModel model = OssieDocumentConverter.importDocument(document);
    sourceValidator.accept(model.definition());
    return importSemanticModel(NameIdentifier.of(namespace, model.name()), model);
  }

  /**
   * Dispatches a parsed import through normalization and import-specific hooks.
   *
   * <p>This internal entry point is used by {@link #importOssieDocument(Namespace, OssieDocument,
   * Consumer)} after parsing and source validation. Decorators forward it separately from create so
   * only the operation dispatcher reuses the underlying create implementation.
   *
   * @param ident The identifier of the model to import.
   * @param model The parsed model.
   * @return The imported Semantic Model.
   */
  default SemanticModel importSemanticModel(NameIdentifier ident, ImportedSemanticModel model) {
    return createSemanticModel(ident, model.comment(), model.definition(), model.properties());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Decorators forward this operation separately from load. The operation dispatcher reuses the
   * underlying load implementation before serializing the model.
   */
  @Override
  default OssieDocument exportOssieDocument(NameIdentifier ident, OssieFormat format) {
    return OssieDocumentConverter.exportDocument(loadSemanticModel(ident), format);
  }
}
