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

/** Represents an event triggered before the linking of a model version. */
@DeveloperApi
public class LinkModelVersionPreEvent extends ModelPreEvent {
  private final ModelInfo modelInfo;

  /**
   * Create a new {@link LinkModelVersionPreEvent} instance.
   *
   * @param user The username of the individual who initiated the model version linking.
   * @param identifier The unique identifier of the model that was linked.
   * @param modelInfo The final state of the model after linking.
   */
  public LinkModelVersionPreEvent(String user, NameIdentifier identifier, ModelInfo modelInfo) {
    super(user, identifier);
    this.modelInfo = modelInfo;
  }

  /**
   * Retrieves the linked model version information.
   *
   * @return the model information.
   */
  public ModelInfo linkedModelInfo() {
    return modelInfo;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LINK_MODEL_VERSION;
  }
}
