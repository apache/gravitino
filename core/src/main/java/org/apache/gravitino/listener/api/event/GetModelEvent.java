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
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.ModelInfo;

/** Represents an event that is generated after a model has been successfully retrieved. */
@DeveloperApi
public class GetModelEvent extends ModelEvent {
  private final ModelInfo modelInfo;

  /**
   * Constructs an instance of {@code GetModelEvent}, capturing essential details about the
   * successful getting of a model.
   *
   * @param user The username of the individual who initiated the model get.
   * @param identifier The unique identifier of the model that was get.
   * @param modelInfo The state of the model post-get.
   */
  public GetModelEvent(String user, NameIdentifier identifier, ModelInfo modelInfo) {
    super(user, identifier);
    this.modelInfo = modelInfo;
  }

  /**
   * Retrieves the state of the model as it was made available to the user after successful getting.
   *
   * @return A {@link ModelInfo} instance encapsulating the details of the model as get.
   */
  public ModelInfo modelInfo() {
    return modelInfo;
  }

  /**
   * Returns the type of operation.
   *
   * @return The operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_MODEL;
  }
}
