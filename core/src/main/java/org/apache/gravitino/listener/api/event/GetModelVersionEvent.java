/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.api.info.ModelInfo;

/** Represents an event triggered upon the successful getting the version of a model. */
public class GetModelVersionEvent extends ModelEvent {
  public final ModelInfo getModelVersionInfo;

  /**
   * Constructs an instance of {@code GetModelVersionEvent}.
   *
   * @param user The username of the individual who initiated the get model version event.
   * @param identifier The unique identifier of the model that was getting the version.
   * @param getModelVersionInfo The state of the model after the version was loaded.
   */
  public GetModelVersionEvent(
      String user, NameIdentifier identifier, ModelInfo getModelVersionInfo) {
    super(user, identifier);
    this.getModelVersionInfo = getModelVersionInfo;
  }

  /**
   * Retrieves the state of the model as it was made available to the user after successful getting
   * the version.
   *
   * @return A {@link ModelInfo} instance encapsulating the details of the model version.
   */
  public ModelInfo getModelVersionInfo() {
    return getModelVersionInfo;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_MODEL_VERSION;
  }
}
