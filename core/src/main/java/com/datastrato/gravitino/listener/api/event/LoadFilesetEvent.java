/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;

/** Represents an event that occurs when a fileset is loaded into the system. */
public final class LoadFilesetEvent extends FilesetEvent {
  /**
   * Constructs a new {@code LoadFilesetEvent}.
   *
   * @param user The user who initiated the loading of the fileset.
   * @param identifier The unique identifier of the fileset being loaded.
   */
  public LoadFilesetEvent(String user, NameIdentifier identifier) {
    super(user, identifier);
  }
}
