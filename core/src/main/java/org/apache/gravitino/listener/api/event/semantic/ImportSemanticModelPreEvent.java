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
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.semantic.OssieFormat;

/** Pre-event before importing an Ossie document as a Semantic Model. */
@DeveloperApi
public final class ImportSemanticModelPreEvent extends SemanticModelPreEvent {
  private final Namespace namespace;
  @Nullable private final OssieFormat format;

  /**
   * Constructs a {@code ImportSemanticModelPreEvent}.
   *
   * @param user The username of the individual who initiated the operation.
   * @param namespace The destination schema; the model name may not yet be available.
   * @param format The document format, or null for an invalid request.
   */
  public ImportSemanticModelPreEvent(
      String user, Namespace namespace, @Nullable OssieFormat format) {
    super(user, NameIdentifier.of(namespace.levels()));
    this.namespace = namespace;
    this.format = format;
  }

  /**
   * Returns the destination schema namespace.
   *
   * @return The destination namespace.
   */
  public Namespace namespace() {
    return namespace;
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
    return OperationType.IMPORT_SEMANTIC_MODEL;
  }
}
