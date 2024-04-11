/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;

/** Represents an event that occurs when an attempt to load a fileset into the system fails. */
public final class LoadFilesetFailureEvent extends FilesetFailureEvent {
  /**
   * Constructs a new {@code FilesetFailureEvent} instance.
   *
   * @param user The user associated with the failed table operation.
   * @param identifier The identifier of the table that was involved in the failed operation.
   * @param exception The exception that was thrown during the table operation, indicating the cause
   *     of the failure.
   */
  public LoadFilesetFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
