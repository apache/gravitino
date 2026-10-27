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

import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.semantic.OssieFormat;

/** Pre-event before exporting a Semantic Model as an Ossie document. */
@DeveloperApi
public final class ExportSemanticModelPreEvent extends SemanticModelPreEvent {
  @Nullable private final OssieFormat format;

  /**
   * Constructs a {@code ExportSemanticModelPreEvent}.
   *
   * @param user The username of the individual who initiated the operation.
   * @param identifier The Semantic Model identifier.
   * @param format The document format, or null for an invalid request.
   */
  public ExportSemanticModelPreEvent(
      String user, NameIdentifier identifier, @Nullable OssieFormat format) {
    super(user, identifier);
    this.format = format;
  }

  /**
   * Returns the document format.
   *
   * @return The format, or null for an invalid request.
   */
  @Nullable
  public OssieFormat format() {
    return format;
  }

  @Override
  public OperationType operationType() {
    return OperationType.EXPORT_SEMANTIC_MODEL;
  }
}
