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
import org.apache.gravitino.listener.api.info.ModelVersionInfo;
import org.apache.gravitino.model.ModelVersionChange;

/** Represents an event triggered when a model version is successfully altered. */
@DeveloperApi
public class AlterModelVersionEvent extends ModelEvent {
  private final ModelVersionInfo alteredModelVersionInfo;
  private final ModelVersionChange[] modelVersionChanges;

  /**
   * Constructs a new {@link AlterModelVersionEvent} instance with specified arguments.
   *
   * @param user The user who triggered the event.
   * @param identifier the identifier of the model.
   * @param alteredModelVersionInfo The post-alteration state of the model version.
   * @param modelVersionChanges An array of {@link ModelVersionChange} objects representing the
   *     specific changes applied to the model version during the alteration process.
   */
  public AlterModelVersionEvent(
      String user,
      NameIdentifier identifier,
      ModelVersionInfo alteredModelVersionInfo,
      ModelVersionChange[] modelVersionChanges) {
    super(user, identifier);

    this.alteredModelVersionInfo = alteredModelVersionInfo;
    this.modelVersionChanges = modelVersionChanges;
  }

  /**
   * Retrieves the updated state of the model version after the successful alteration.
   *
   * @return A {@link ModelVersionInfo} instance encapsulating the details of the altered model
   *     version.
   */
  public ModelVersionInfo alteredModelVersionInfo() {
    return alteredModelVersionInfo;
  }

  /**
   * Retrieves the specific changes that were made to the model version during the alteration
   * process.
   *
   * @return An array of {@link ModelVersionChange} objects detailing each modification applied to
   *     the specified model version.
   */
  public ModelVersionChange[] modelVersionChanges() {
    return modelVersionChanges;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ALTER_MODEL_VERSION;
  }
}
