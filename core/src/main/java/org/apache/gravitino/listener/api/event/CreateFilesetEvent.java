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

/** Represents an event that is triggered following the successful creation of a fileset. */
@DeveloperApi
public final class CreateFilesetEvent extends FilesetEvent {
  private final FilesetInfo createdFilesetInfo;

  /**
   * Constructs a new {@code CreateFilesetEvent}, capturing the essential details surrounding the
   * successful creation of a fileset.
   *
   * @param user The username of the person who initiated the creation of the fileset.
   * @param identifier The unique identifier of the newly created fileset.
   * @param createdFilesetInfo The state of the fileset immediately following its creation,
   *     including details such as its location, structure, and access permissions.
   */
  public CreateFilesetEvent(
      String user, NameIdentifier identifier, FilesetInfo createdFilesetInfo) {
    super(user, identifier);
    this.createdFilesetInfo = createdFilesetInfo;
  }

  /**
   * Provides information about the fileset as it was configured at the moment of creation.
   *
   * @return A {@link FilesetInfo} object encapsulating the state of the fileset immediately after
   *     its creation.
   */
  public FilesetInfo createdFilesetInfo() {
    return createdFilesetInfo;
  }
}
