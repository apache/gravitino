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
import org.apache.gravitino.model.ModelChange;

/**
 * Represents an event that is triggered when an attempt to alter a model fails due to an exception.
 */
public class AlterModelFailureEvent extends ModelFailureEvent {
  private final ModelChange[] modelChanges;

  /**
   * Constructs a new {@link AlterModelFailureEvent} instance with the specified user, identifier,
   * exception, and model changes.
   *
   * @param user The user who initiated the model alteration operation.
   * @param identifier The identifier of the model that was attempted to be altered.
   * @param exception The exception that was thrown during the model alteration operation.
   * @param modelChanges The changes that were attempted on the model.
   */
  public AlterModelFailureEvent(
      String user, NameIdentifier identifier, Exception exception, ModelChange[] modelChanges) {
    super(user, identifier, exception);
    this.modelChanges = modelChanges;
  }

  /**
   * Retrieves the changes that were attempted on the model.
   *
   * @return An array of {@link ModelChange} objects detailing each modification applied to the
   *     model.
   */
  public ModelChange[] modelChanges() {
    return modelChanges;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ALTER_MODEL;
  }
}
