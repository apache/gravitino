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
import org.apache.gravitino.audit.CallerContext;

/** Represents an event that occurs when getting a actual file location. */
@DeveloperApi
public final class GetFileLocationEvent extends FilesetEvent {
  private final String actualFileLocation;

  private final CallerContext callerContext;

  /**
   * Constructs a new {@code GetFileLocationEvent}, recording the attempt to get a file location.
   *
   * @param user The user who initiated the get file location.
   * @param identifier The identifier of the file location that was attempted to be got.
   * @param callerContext The audit caller context, this param can be null.
   * @param actualFileLocation The actual file location which want to get.
   */
  public GetFileLocationEvent(
      String user,
      NameIdentifier identifier,
      String actualFileLocation,
      CallerContext callerContext) {
    super(user, identifier);
    this.actualFileLocation = actualFileLocation;
    this.callerContext = callerContext;
  }

  public String actualFileLocation() {
    return actualFileLocation;
  }

  public CallerContext callerContext() {
    return callerContext;
  }
}
