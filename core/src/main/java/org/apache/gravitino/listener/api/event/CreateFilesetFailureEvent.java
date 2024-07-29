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
import org.apache.gravitino.listener.api.info.FilesetInfo;

/** Represents an event triggered upon the unsuccessful attempt to create a fileset. */
@DeveloperApi
public final class CreateFilesetFailureEvent extends FilesetFailureEvent {
  private final FilesetInfo createFilesetRequest;

  /**
   * Constructs a new {@code CreateFilesetFailureEvent}, capturing the specifics of the failed
   * fileset creation attempt.
   *
   * @param user The user who initiated the attempt to create the fileset.
   * @param identifier The identifier of the fileset intended for creation.
   * @param exception The exception encountered during the fileset creation process, shedding light
   *     on the potential reasons behind the failure.
   * @param createFilesetRequest The original request information used to attempt to create the
   *     fileset.
   */
  public CreateFilesetFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      FilesetInfo createFilesetRequest) {
    super(user, identifier, exception);
    this.createFilesetRequest = createFilesetRequest;
  }

  /**
   * Provides insight into the intended configuration of the fileset at the time of the failed
   * creation attempt.
   *
   * @return The {@link FilesetInfo} instance representing the request information for the failed
   *     fileset creation attempt.
   */
  public FilesetInfo createFilesetRequest() {
    return createFilesetRequest;
  }
}
