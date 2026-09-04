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
import org.apache.gravitino.semantic.SemanticModelChange;

/** Pre-event before altering a Semantic Model. */
@DeveloperApi
public class AlterSemanticModelPreEvent extends SemanticModelPreEvent {
  private final SemanticModelChange[] semanticModelChanges;

  /**
   * Constructs an instance of {@code AlterSemanticModelPreEvent}.
   *
   * @param user The username of the individual who initiated the alteration.
   * @param identifier The identifier of the Semantic Model being altered.
   * @param semanticModelChanges The changes to apply.
   */
  public AlterSemanticModelPreEvent(
      String user, NameIdentifier identifier, SemanticModelChange[] semanticModelChanges) {
    super(user, identifier);
    this.semanticModelChanges = semanticModelChanges.clone();
  }

  /**
   * Returns the changes to apply to the Semantic Model.
   *
   * @return The Semantic Model changes.
   */
  public SemanticModelChange[] semanticModelChanges() {
    return semanticModelChanges;
  }

  @Override
  public OperationType operationType() {
    return OperationType.ALTER_SEMANTIC_MODEL;
  }
}
