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

import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered when an attempt to alter a metalake fails due to an
 * exception.
 */
@DeveloperApi
public final class AlterMetalakeFailureEvent extends MetalakeFailureEvent {
  private final MetalakeChange[] metalakeChanges;

  /**
   * Constructs an {@code AlterMetalakeFailureEvent} instance, capturing detailed information about
   * the failed metalake alteration attempt.
   *
   * @param user The user who initiated the metalake alteration operation.
   * @param identifier The identifier of the metalake that was attempted to be altered.
   * @param exception The exception that was thrown during the metalake alteration operation.
   * @param metalakeChanges The changes that were attempted on the metalake.
   */
  public AlterMetalakeFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      MetalakeChange[] metalakeChanges) {
    super(user, identifier, exception);
    this.metalakeChanges = metalakeChanges.clone();
  }

  /**
   * Retrieves the changes that were attempted on the metalake.
   *
   * @return An array of {@link MetalakeChange} objects representing the attempted modifications to
   *     the metalake.
   */
  public MetalakeChange[] metalakeChanges() {
    return metalakeChanges;
  }
}
