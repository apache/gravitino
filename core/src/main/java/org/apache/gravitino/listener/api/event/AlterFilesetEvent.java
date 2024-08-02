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
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.listener.api.info.FilesetInfo;

/** Represents an event that occurs when a fileset is altered. */
@DeveloperApi
public final class AlterFilesetEvent extends FilesetEvent {
  private final FilesetInfo updatedFilesetInfo;
  private final FilesetChange[] filesetChanges;

  /**
   * Constructs a new {@code AlterFilesetEvent} instance.
   *
   * @param user The username of the individual who initiated the fileset alteration.
   * @param identifier The unique identifier of the fileset that was altered.
   * @param filesetChanges An array of {@link FilesetChange} objects representing the specific
   *     changes applied to the fileset.
   * @param updatedFilesetInfo The {@link FilesetInfo} object representing the state of the fileset
   *     after the changes were applied.
   */
  public AlterFilesetEvent(
      String user,
      NameIdentifier identifier,
      FilesetChange[] filesetChanges,
      FilesetInfo updatedFilesetInfo) {
    super(user, identifier);
    this.filesetChanges = filesetChanges.clone();
    this.updatedFilesetInfo = updatedFilesetInfo;
  }

  /**
   * Retrieves the array of changes made to the fileset.
   *
   * @return An array of {@link FilesetChange} objects detailing the modifications applied to the
   *     fileset.
   */
  public FilesetChange[] filesetChanges() {
    return filesetChanges;
  }

  /**
   * Retrieves the information about the fileset after the alterations.
   *
   * @return A {@link FilesetInfo} object representing the current state of the fileset
   *     post-alteration.
   */
  public FilesetInfo updatedFilesetInfo() {
    return updatedFilesetInfo;
  }
}
