/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated when an attempt to get a file location from the system
 * fails.
 */
@DeveloperApi
public final class GetFileLocationFailureEvent extends FilesetFailureEvent {
  private final String subPath;

  /**
   * Constructs a new {@code GetFileLocationFailureEvent}.
   *
   * @param user The user who initiated the get a file location.
   * @param identifier The identifier of the file location that was attempted to be got.
   * @param subPath The sub path of the actual file location which want to get.
   * @param exception The exception that was thrown during the get a file location. This exception
   *     is key to diagnosing the failure, providing insights into what went wrong during the
   *     operation.
   */
  public GetFileLocationFailureEvent(
      String user, NameIdentifier identifier, String subPath, Exception exception) {
    super(user, identifier, exception);
    this.subPath = subPath;
  }

  /**
   * Get the audit context map of the get file location operation.
   *
   * @return The audit context map.
   */
  public String subPath() {
    return subPath;
  }
}
