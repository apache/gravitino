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

/** Successful load-Semantic-Model event. */
@DeveloperApi
public final class LoadSemanticModelEvent extends SemanticModelEvent {
  private final SemanticModelInfo loadedSemanticModelInfo;

  /**
   * Constructs an instance of {@code LoadSemanticModelEvent}.
   *
   * @param user The username of the individual who initiated the load.
   * @param identifier The identifier of the loaded Semantic Model.
   * @param loadedSemanticModelInfo The loaded Semantic Model information.
   */
  public LoadSemanticModelEvent(
      String user, NameIdentifier identifier, SemanticModelInfo loadedSemanticModelInfo) {
    super(user, identifier);
    this.loadedSemanticModelInfo = loadedSemanticModelInfo;
  }

  /**
   * Returns the loaded Semantic Model information.
   *
   * @return The loaded Semantic Model information.
   */
  public SemanticModelInfo loadedSemanticModelInfo() {
    return loadedSemanticModelInfo;
  }

  @Override
  public OperationType operationType() {
    return OperationType.LOAD_SEMANTIC_MODEL;
  }
}
