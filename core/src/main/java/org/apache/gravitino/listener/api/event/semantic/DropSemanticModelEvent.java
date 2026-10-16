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

/** Successful drop-Semantic-Model event. */
@DeveloperApi
public final class DropSemanticModelEvent extends SemanticModelEvent {
  private final boolean isExists;

  /**
   * Constructs an instance of {@code DropSemanticModelEvent}.
   *
   * @param user The username of the individual who initiated the drop.
   * @param identifier The identifier of the dropped Semantic Model.
   * @param isExists Whether the Semantic Model existed when the drop was applied.
   */
  public DropSemanticModelEvent(String user, NameIdentifier identifier, boolean isExists) {
    super(user, identifier);
    this.isExists = isExists;
  }

  /**
   * Returns whether the Semantic Model existed when the drop was applied.
   *
   * @return {@code true} if the Semantic Model existed, otherwise {@code false}.
   */
  public boolean isExists() {
    return isExists;
  }

  @Override
  public OperationType operationType() {
    return OperationType.DROP_SEMANTIC_MODEL;
  }
}
