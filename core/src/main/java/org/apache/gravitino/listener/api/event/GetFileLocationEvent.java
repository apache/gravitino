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

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represents an event that occurs when getting an actual file location. */
@DeveloperApi
public final class GetFileLocationEvent extends FilesetEvent {
  private final String actualFileLocation;
  private final String subPath;
  private final Map<String, String> context;

  /**
   * Constructs a new {@code GetFileLocationEvent}, recording the attempt to get a file location.
   *
   * @param user The user who initiated the get file location.
   * @param identifier The identifier of the file location that was attempted to be got.
   * @param actualFileLocation The actual file location which want to get.
   * @param subPath The accessing sub path of the get file location operation.
   * @param context The audit context, this param can be null.
   */
  public GetFileLocationEvent(
      String user,
      NameIdentifier identifier,
      String actualFileLocation,
      String subPath,
      Map<String, String> context) {
    super(user, identifier);
    this.actualFileLocation = actualFileLocation;
    this.subPath = subPath;
    this.context = context;
  }

  /**
   * Get the actual file location after processing of the get file location operation.
   *
   * @return The actual file location.
   */
  public String actualFileLocation() {
    return actualFileLocation;
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
   * Get the audit context map of the get file location operation.
   *
   * @return The audit context map.
   */
  public Map<String, String> context() {
    return context;
  }
}
