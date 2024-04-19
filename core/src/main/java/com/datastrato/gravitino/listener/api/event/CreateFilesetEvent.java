/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.FilesetInfo;

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
