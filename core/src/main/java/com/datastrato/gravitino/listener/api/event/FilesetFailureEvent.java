/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * An abstract class representing events that are triggered when a fileset operation fails due to an
 * exception.
 *
 * <p>Implementations of this class can be used to convey detailed information about failures in
 * operations such as creating, updating, deleting, or querying filesets, making it easier to
 * diagnose and respond to issues.
 */
@DeveloperApi
public abstract class FilesetFailureEvent extends FailureEvent {
  /**
   * Constructs a new {@code FilesetFailureEvent} instance, capturing information about the failed
   * fileset operation.
   *
   * @param user The user associated with the failed fileset operation.
   * @param identifier The identifier of the fileset that was involved in the failed operation.
   * @param exception The exception that was thrown during the fileset operation, indicating the
   *     cause of the failure.
   */
  protected FilesetFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
