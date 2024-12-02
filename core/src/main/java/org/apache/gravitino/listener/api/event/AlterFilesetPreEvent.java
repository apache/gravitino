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

/** Represents an event triggered before altering a fileset. */
@DeveloperApi
public final class AlterFilesetPreEvent extends FilesetPreEvent {
  private final FilesetChange[] filesetChanges;

  public AlterFilesetPreEvent(
      String user, NameIdentifier identifier, FilesetChange[] filesetChanges) {
    super(user, identifier);
    this.filesetChanges = filesetChanges;
  }

  /**
   * Retrieves the specific changes that were made to the fileset during the alteration process.
   *
   * @return An array of {@link FilesetChange} objects detailing each modification applied to the
   *     fileset.
   */
  public FilesetChange[] filesetChanges() {
    return filesetChanges;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ALTER_FILESET;
  }
}
