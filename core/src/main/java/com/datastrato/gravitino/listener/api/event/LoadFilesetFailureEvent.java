/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/** Represents an event that occurs when an attempt to load a fileset into the system fails. */
@DeveloperApi
public final class LoadFilesetFailureEvent extends FilesetFailureEvent {
  /**
   * Constructs a new {@code FilesetFailureEvent} instance.
   *
   * @param user The user associated with the failed fileset operation.
   * @param identifier The identifier of the fileset that was involved in the failed operation.
   * @param exception The exception that was thrown during the fileset operation, indicating the
   *     cause of the failure.
   */
  public LoadFilesetFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
