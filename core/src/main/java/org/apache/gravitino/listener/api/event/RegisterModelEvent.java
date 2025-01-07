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

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.api.info.ModelInfo;

/** Represents an event triggered upon the successful registration of a model. */
public class RegisterModelEvent extends ModelEvent {
  private final ModelInfo registeredTopicInfo;

  /**
   * Constructs an instance of RegisterModelEvent, capturing essential details about the successful
   * registration of a model.
   *
   * @param user The user responsible for triggering the model operation.
   * @param identifier The identifier of the Model involved in the operation. This encapsulates
   *     details such as the metalake, catalog, schema, and Model name.
   * @param registeredTopicInfo The final state of the model post-creation.
   */
  protected RegisterModelEvent(
      String user, NameIdentifier identifier, ModelInfo registeredTopicInfo) {
    super(user, identifier);
    this.registeredTopicInfo = registeredTopicInfo;
  }

  /**
   * Retrieves the final state of the model as it was returned to the user after successful
   * registration.
   *
   * @return the model information.
   */
  public ModelInfo registeredModelInfo() {
    return registeredTopicInfo;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.REGISTER_MODEL;
  }
}
