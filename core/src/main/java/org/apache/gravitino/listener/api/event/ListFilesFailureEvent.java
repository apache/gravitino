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

@DeveloperApi
public class ListFilesFailureEvent extends FilesetFailureEvent {
  private final String locationName;
  private final String subPath;

  /**
   * Constructs a new {@code ListFilesFailureEvent}.
   *
   * @param user The user who initiated the listing of files/directories under the fileset.
   * @param ident The identifier of the fileset.
   * @param locationName The name of the location.
   * @param subPath The subPath of the fileset.
   * @param exception The exception encountered during the files listing attempt, which serves as an
   *     indicator of the issues that caused the failure.
   */
  public ListFilesFailureEvent(
      String user, NameIdentifier ident, String locationName, String subPath, Exception exception) {
    super(user, ident, exception);
    this.locationName = locationName;
    this.subPath = subPath;
  }

  public String locationName() {
    return locationName;
  }

  public String subPath() {
    return subPath;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_FILESET_FILES;
  }
}
