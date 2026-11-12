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

package org.apache.gravitino.listener.api.event.semantic;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.info.SemanticModelInfo;
import org.apache.gravitino.semantic.OssieFormat;

/** Successful event for importing an Ossie document as a Semantic Model. */
@DeveloperApi
public final class ImportSemanticModelEvent extends SemanticModelEvent {
  private final OssieFormat format;
  private final SemanticModelInfo importedSemanticModelInfo;

  /**
   * Constructs a {@code ImportSemanticModelEvent}.
   *
   * @param user The username of the individual who initiated the operation.
   * @param identifier The Semantic Model identifier.
   * @param format The non-null document format.
   * @param importedSemanticModelInfo The imported Semantic Model information.
   */
  public ImportSemanticModelEvent(
      String user,
      NameIdentifier identifier,
      OssieFormat format,
      SemanticModelInfo importedSemanticModelInfo) {
    super(user, identifier);
    this.format = format;
    this.importedSemanticModelInfo = importedSemanticModelInfo;
  }

  /**
   * Returns the document format.
   *
   * @return The non-null document format.
   */
  public OssieFormat format() {
    return format;
  }

  /**
   * Returns the imported Semantic Model information.
   *
   * @return The imported Semantic Model information.
   */
  public SemanticModelInfo importedSemanticModelInfo() {
    return importedSemanticModelInfo;
  }

  @Override
  public OperationType operationType() {
    return OperationType.IMPORT_SEMANTIC_MODEL;
  }
}
