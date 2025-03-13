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

/**
 * Represents an event that is generated when an attempt to drop a model from the schema fails due
 * to an exception.
 */
@DeveloperApi
public class DeleteModelFailureEvent extends ModelFailureEvent {
  /**
   * Construct a new {@link DeleteModelFailureEvent} instance, capturing detailed information about
   * the failed attempt to drop a model.
   *
   * @param user The user who initiated the drop model operation.
   * @param identifier The identifier of the model that the operation attempted to drop.
   * @param exception The exception that was thrown during the drop model operation, offering
   *     insights into what went wrong and why the operation failed.
   */
  public DeleteModelFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.DELETE_MODEL;
  }
}
