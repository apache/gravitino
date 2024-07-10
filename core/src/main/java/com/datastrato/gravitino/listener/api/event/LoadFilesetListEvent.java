/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.FilesetInfo;

/** Represents an event that occurs when filesets are loaded into the system. */
@DeveloperApi
public final class LoadFilesetListEvent extends FilesetEvent {
  private final FilesetInfo[] loadedFilesetInfos;
  /**
   * Constructs a new {@code LoadFilesetListEvent}.
   *
   * @param user The user who initiated the loading of the fileset.
   * @param identifier The unique identifier of the fileset being loaded.
   * @param loadedFilesetInfos The state of the fileset post-loading.
   */
  public LoadFilesetListEvent(
      String user, NameIdentifier identifier, FilesetInfo[] loadedFilesetInfos) {
    super(user, identifier);
    this.loadedFilesetInfos = loadedFilesetInfos;
  }

  /**
   * Retrieves the state of the filesets as it was made available to the user after successful
   * loading.
   *
   * @return A {@link FilesetInfo} instance encapsulating the details of the fileset as loaded.
   */
  public FilesetInfo[] loadedFilesetInfo() {
    return loadedFilesetInfos;
  }
}
