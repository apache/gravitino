/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.listener.api.info.FilesetInfo;

/** Represents an event that occurs when a fileset is altered. */
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
