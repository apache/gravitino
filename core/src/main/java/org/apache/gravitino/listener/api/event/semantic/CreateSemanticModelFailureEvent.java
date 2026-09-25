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

/** Failure event for creating a Semantic Model. */
@DeveloperApi
public final class CreateSemanticModelFailureEvent extends SemanticModelFailureEvent {
  private final SemanticModelInfo createSemanticModelRequest;

  /**
   * Constructs an instance of {@code CreateSemanticModelFailureEvent}.
   *
   * @param user The username of the individual who initiated the creation.
   * @param identifier The identifier of the Semantic Model being created.
   * @param exception The exception that caused the failure.
   * @param createSemanticModelRequest The requested Semantic Model information.
   */
  public CreateSemanticModelFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      SemanticModelInfo createSemanticModelRequest) {
    super(user, identifier, exception);
    this.createSemanticModelRequest = createSemanticModelRequest;
  }

  /**
   * Returns the requested Semantic Model information.
   *
   * @return The requested Semantic Model information.
   */
  public SemanticModelInfo createSemanticModelRequest() {
    return createSemanticModelRequest;
  }

  @Override
  public OperationType operationType() {
    return OperationType.CREATE_SEMANTIC_MODEL;
  }
}
