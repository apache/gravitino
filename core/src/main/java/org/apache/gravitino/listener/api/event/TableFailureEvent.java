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
 * An abstract class representing events that are triggered when a table operation fails due to an
 * exception. This class extends {@link FailureEvent} to provide a more specific context related to
 * table operations, encapsulating details about the user who initiated the operation, the
 * identifier of the table involved, and the exception that led to the failure.
 *
 * <p>Implementations of this class can be used to convey detailed information about failures in
 * operations such as creating, updating, deleting, or querying tables, making it easier to diagnose
 * and respond to issues.
 */
@DeveloperApi
public abstract class TableFailureEvent extends FailureEvent {
  /**
   * Constructs a new {@code TableFailureEvent} instance, capturing information about the failed
   * table operation.
   *
   * @param user The user associated with the failed table operation.
   * @param identifier The identifier of the table that was involved in the failed operation.
   * @param exception The exception that was thrown during the table operation, indicating the cause
   *     of the failure.
   */
  protected TableFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
