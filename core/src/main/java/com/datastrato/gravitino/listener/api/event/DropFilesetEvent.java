/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;

/** Represents an event that occurs when a fileset is dropped from the system. */
public final class DropFilesetEvent extends FilesetEvent {
  private final boolean isExists;

  /**
   * Constructs a new {@code DropFilesetEvent}, recording the attempt to drop a fileset.
   *
   * @param user The user who initiated the drop fileset operation.
   * @param identifier The identifier of the fileset that was attempted to be dropped.
   * @param isExists A boolean flag indicating whether the fileset existed at the time of the
   *     operation.
   */
  public DropFilesetEvent(String user, NameIdentifier identifier, boolean isExists) {
    super(user, identifier);
    this.isExists = isExists;
  }

  /**
   * Retrieves the existence status of the fileset at the time of the drop operation.
   *
   * @return {@code true} if the fileset existed at the time of the operation, otherwise {@code
   *     false}.
   */
  public boolean isExists() {
    return isExists;
  }
}
