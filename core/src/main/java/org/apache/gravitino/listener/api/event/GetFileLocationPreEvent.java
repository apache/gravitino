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

/** Represents an event that is triggered before attempting to get an actual file location. */
@DeveloperApi
public final class GetFileLocationPreEvent extends FilesetPreEvent {
  private final String subPath;
  private final String locationName;

  /**
   * Constructs a new {@code GetFileLocationPreEvent}, recording the intent to get a file location.
   *
   * @param user The user who initiated the get file location operation.
   * @param identifier The identifier of the file location to be accessed.
   * @param subPath The accessing sub path of the get file location operation.
   */
  public GetFileLocationPreEvent(String user, NameIdentifier identifier, String subPath) {
    this(user, identifier, subPath, null);
  }

  /**
   * Constructs a new {@code GetFileLocationPreEvent}, recording the intent to get a file location.
   *
   * @param user The user who initiated the get file location operation.
   * @param identifier The identifier of the file location to be accessed.
   * @param subPath The accessing sub path of the get file location operation.
   * @param locationName The name of the location to be accessed.
   */
  public GetFileLocationPreEvent(
      String user, NameIdentifier identifier, String subPath, String locationName) {
    super(user, identifier);
    this.subPath = subPath;
    this.locationName = locationName;
  }

  /**
   * Get the accessing sub path of the get file location operation.
   *
   * @return The accessing sub path.
   */
  public String subPath() {
    return subPath;
  }

  /**
   * Get the name of the location to be accessed.
   *
   * @return The name of the location.
   */
  public String locationName() {
    return locationName;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_FILESET_LOCATION;
  }
}
