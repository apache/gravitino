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

/** Pre-event before loading a Semantic Model. */
@DeveloperApi
public class LoadSemanticModelPreEvent extends SemanticModelPreEvent {

  /**
   * Constructs an instance of {@code LoadSemanticModelPreEvent}.
   *
   * @param user The username of the individual who initiated the load.
   * @param identifier The identifier of the Semantic Model being loaded.
   */
  public LoadSemanticModelPreEvent(String user, NameIdentifier identifier) {
    super(user, identifier);
  }

  @Override
  public OperationType operationType() {
    return OperationType.LOAD_SEMANTIC_MODEL;
  }
}
