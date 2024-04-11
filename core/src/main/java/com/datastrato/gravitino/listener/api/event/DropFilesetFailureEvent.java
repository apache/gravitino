/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;

/**
 * Represents an event that is generated when an attempt to drop a fileset from the system fails.
 */
public final class DropFilesetFailureEvent extends FilesetFailureEvent {
  /**
   * Constructs a new {@code DropFilesetFailureEvent}.
   *
   * @param user The user who initiated the drop fileset operation.
   * @param identifier The identifier of the fileset that was attempted to be dropped.
   * @param exception The exception that was thrown during the drop operation. This exception is key
   *     to diagnosing the failure, providing insights into what went wrong during the operation.
   */
  public DropFilesetFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
