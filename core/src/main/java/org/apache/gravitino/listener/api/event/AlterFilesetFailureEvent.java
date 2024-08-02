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

/**
 * Represents an event that is generated when an attempt to alter a fileset fails due to an
 * exception.
 */
@DeveloperApi
public final class AlterFilesetFailureEvent extends FilesetFailureEvent {
  private final FilesetChange[] filesetChanges;

  /**
   * Constructs a new {@code AlterFilesetFailureEvent}, capturing detailed information about the
   * failed attempt to alter a fileset.
   *
   * @param user The user who initiated the fileset alteration operation.
   * @param identifier The identifier of the fileset that was attempted to be altered.
   * @param exception The exception that was encountered during the alteration attempt, providing
   *     insight into the cause of the failure.
   * @param filesetChanges An array of {@link FilesetChange} objects representing the changes that
   *     were attempted on the fileset.
   */
  public AlterFilesetFailureEvent(
      String user, NameIdentifier identifier, Exception exception, FilesetChange[] filesetChanges) {
    super(user, identifier, exception);
    this.filesetChanges = filesetChanges.clone();
  }

  /**
   * Retrieves the changes that were attempted on the fileset, leading to the failure.
   *
   * @return An array of {@link FilesetChange} objects detailing the modifications that were
   *     attempted on the fileset.
   */
  public FilesetChange[] filesetChanges() {
    return filesetChanges;
  }
}
