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
 * Represents an event that is triggered when a user operation fails due to an exception. This event
 * captures the user information, a unique identifier for the operation, and the exception that
 * caused the failure.
 */
@DeveloperApi
public abstract class UserFailureEvent extends FailureEvent {

  /**
   * Constructs a new {@code UserFailureEvent} instance.
   *
   * @param user the user who initiated the operation
   * @param identifier the unique identifier associated with the user operation
   * @param exception the exception encountered during the operation, providing details about the
   *     failure
   */
  protected UserFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
