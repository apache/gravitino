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

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.semantic.OssieFormat;

/** Successful event for exporting a Semantic Model as an Ossie document. */
@DeveloperApi
public final class ExportSemanticModelEvent extends SemanticModelEvent {
  private final OssieFormat format;
  private final int revision;

  /**
   * Constructs a {@code ExportSemanticModelEvent}.
   *
   * @param user The username of the individual who initiated the operation.
   * @param identifier The Semantic Model identifier.
   * @param format The non-null document format.
   * @param revision The exported revision number.
   */
  public ExportSemanticModelEvent(
      String user, NameIdentifier identifier, OssieFormat format, int revision) {
    super(user, identifier);
    this.format = format;
    this.revision = revision;
  }

  /**
   * Returns the document format.
   *
   * @return The non-null document format.
   */
  public OssieFormat format() {
    return format;
  }

  /** Returns the exported revision number. */
  public int revision() {
    return revision;
  }

  @Override
  protected Map<String, String> ownCustomInfo() {
    return Map.of("revision", Integer.toString(revision));
  }

  @Override
  public OperationType operationType() {
    return OperationType.EXPORT_SEMANTIC_MODEL;
  }
}
