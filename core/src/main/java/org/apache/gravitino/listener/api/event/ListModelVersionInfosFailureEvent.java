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

public class ListModelVersionInfosFailureEvent extends FailureEvent {
  /**
   * Constructs a new {@code FailureEvent} instance with the specified user, resource identifier,
   * and the exception that was thrown.
   *
   * @param user The user associated with the operation that resulted in a failure.
   * @param identifier The identifier of the resource involved in the operation that failed. This
   *     provides a clear reference to what was being acted upon when the exception occurred.
   * @param exception The exception that was thrown during the operation. This is the primary piece
   *     of information indicating what went wrong.
   */
  public ListModelVersionInfosFailureEvent(
      String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }

  /** {@inheritDoc} */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_MODEL_VERSION_INFOS;
  }
}
