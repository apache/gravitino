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

/**
 * Represents an event that is generated after a model version is successfully dropped from the
 * model.
 */
public class DropModelVersionEvent extends ModelEvent {

  private final ModelInfo dropModelVersionInfo;
  private final boolean isExists;

  /**
   * Constructs a new {@code DropModelVersionEvent} instance, encapsulating information about the
   * outcome of a model version drop operation.
   *
   * @param user The user who initiated the drop model version operation.
   * @param identifier The identifier of the model that was attempted to be dropped a version.
   * @param isExists A boolean flag indicating whether the model version existed at the time of the
   *     drop operation.
   */
  public DropModelVersionEvent(
      String user, NameIdentifier identifier, ModelInfo dropModelVersionInfo, boolean isExists) {
    super(user, identifier);
    this.dropModelVersionInfo = dropModelVersionInfo;
    this.isExists = isExists;
  }

  /**
   * Retrieves the state of the model after the drop version operation.
   *
   * @return The state of the model after the drop version operation.
   */
  public ModelInfo DropModelVersionInfo() {
    return dropModelVersionInfo;
  }

  /**
   * Retrieves the existence status of the model version at the time of the drop operation.
   *
   * @return A boolean value indicating whether the model version existed. {@code true} if the table
   *     existed, otherwise {@code false}.
   */
  public boolean isExists() {
    return isExists;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.DROP_MODEL_VERSION;
  }
}
