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

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;

/** Source-scoped Semantic Model dataset lookup failure event. */
@DeveloperApi
public final class ListSemanticModelDatasetsBySourceFailureEvent extends SemanticModelFailureEvent {
  private final MetadataObject.Type sourceType;

  /**
   * Creates a dataset lookup event.
   *
   * @param user The caller.
   * @param source The fully qualified source identifier.
   * @param sourceType The queried table or View type.
   * @param exception The failure.
   */
  public ListSemanticModelDatasetsBySourceFailureEvent(
      String user, NameIdentifier source, MetadataObject.Type sourceType, Exception exception) {
    super(user, source, exception);
    this.sourceType = sourceType;
  }

  /**
   * @return The type of the source associated with this lookup.
   */
  public MetadataObject.Type sourceType() {
    return sourceType;
  }

  @Override
  public OperationType operationType() {
    return OperationType.LIST_SEMANTIC_MODEL_DATASETS_BY_SOURCE;
  }
}
