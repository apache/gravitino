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

package org.apache.gravitino.listener.api.event.function;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.listener.api.event.OperationType;

/**
 * Represents an event that is triggered when an attempt to alter a function fails due to an
 * exception.
 */
@DeveloperApi
public final class AlterFunctionFailureEvent extends FunctionFailureEvent {
  private final FunctionChange[] functionChanges;

  /**
   * Constructs an {@code AlterFunctionFailureEvent} instance.
   *
   * @param user The user who initiated the function alteration operation.
   * @param identifier The identifier of the function that was attempted to be altered.
   * @param exception The exception that was thrown during the function alteration operation.
   * @param functionChanges The changes that were attempted on the function.
   */
  public AlterFunctionFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      FunctionChange[] functionChanges) {
    super(user, identifier, exception);
    this.functionChanges = functionChanges.clone();
  }

  /**
   * Retrieves the changes that were attempted on the function.
   *
   * @return An array of {@link FunctionChange} objects representing the attempted modifications to
   *     the function.
   */
  public FunctionChange[] functionChanges() {
    return functionChanges;
  }

  @Override
  public OperationType operationType() {
    return OperationType.ALTER_FUNCTION;
  }
}
