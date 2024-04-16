/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.FilesetInfo;

/** Represents an event that occurs when a fileset is loaded into the system. */
@DeveloperApi
public final class LoadFilesetEvent extends FilesetEvent {
  private final FilesetInfo loadedFilesetInfo;
  /**
   * Constructs a new {@code LoadFilesetEvent}.
   *
   * @param user The user who initiated the loading of the fileset.
   * @param identifier The unique identifier of the fileset being loaded.
   * @param loadedFilesetInfo The state of the fileset post-loading.
   */
  public LoadFilesetEvent(String user, NameIdentifier identifier, FilesetInfo loadedFilesetInfo) {
    super(user, identifier);
    this.loadedFilesetInfo = loadedFilesetInfo;
  }

  /**
   * Retrieves the state of the fileset as it was made available to the user after successful
   * loading.
   *
   * @return A {@link FilesetInfo} instance encapsulating the details of the fileset as loaded.
   */
  public FilesetInfo loadedFilesetInfo() {
    return loadedFilesetInfo;
  }
}
