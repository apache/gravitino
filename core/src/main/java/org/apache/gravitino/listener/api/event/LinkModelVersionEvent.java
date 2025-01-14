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

/** Represents an event triggered upon the successful linking of a model version. */
public class LinkModelVersionEvent extends ModelEvent {
  private ModelInfo modelInfo;

  /**
   * Constructs an instance of {@code LinkModelVersionEvent}, capturing essential details about the
   * successful linking of a model version.
   *
   * @param user The username of the individual who initiated the model version linking.
   * @param identifier The unique identifier of the model that was linked.
   * @param modelInfo The final state of the model after linking.
   */
  public LinkModelVersionEvent(String user, NameIdentifier identifier, ModelInfo modelInfo) {
    super(user, identifier);
    this.modelInfo = modelInfo;
  }

  /**
   * Retrieves the final state of the model, as it was returned to the user after successful link a
   * model version.
   *
   * @return A {@link ModelInfo} instance encapsulating the comprehensive details of the newly model
   *     version.
   */
  public ModelInfo linkModelVersionInfo() {
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
