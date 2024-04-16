/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.file.FilesetChange;

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
