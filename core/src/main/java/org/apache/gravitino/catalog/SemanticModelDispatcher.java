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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.requests.SemanticModelCreateRequest;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.semantic.OssieSemanticModelDocumentConverter;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelCatalog;

/** A dispatcher specialization for schema-scoped Semantic Model operations. */
public interface SemanticModelDispatcher extends SemanticModelCatalog {

  /**
   * {@inheritDoc}
   *
   * <p>Delegates to this dispatcher's native create operation so that the document follows the same
   * normalization and validation chain as a structured create request.
   */
  @Override
  default SemanticModel importOssieSemanticModel(Namespace namespace, OssieDocument document) {
    SemanticModelCreateRequest request =
        OssieSemanticModelDocumentConverter.importDocument(document);
    return createSemanticModel(
        NameIdentifier.of(namespace, request.getName()),
        request.getComment(),
        request.toDefinition(),
        request.getProperties());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Loads the model through this dispatcher's native load operation before serializing it.
   */
  @Override
  default OssieDocument exportOssieSemanticModel(NameIdentifier ident, OssieFormat format) {
    return OssieSemanticModelDocumentConverter.exportDocument(loadSemanticModel(ident), format);
  }
}
