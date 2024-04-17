/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * An abstract class representing events that are triggered when a table operation fails due to an
 * exception. This class extends {@link FailureEvent} to provide a more specific context related to
 * table operations, encapsulating details about the user who initiated the operation, the
 * identifier of the table involved, and the exception that led to the failure.
 *
 * <p>Implementations of this class can be used to convey detailed information about failures in
 * operations such as creating, updating, deleting, or querying tables, making it easier to diagnose
 * and respond to issues.
 */
@DeveloperApi
public abstract class TableFailureEvent extends FailureEvent {
  /**
   * Constructs a new {@code TableFailureEvent} instance, capturing information about the failed
   * table operation.
   *
   * @param user The user associated with the failed table operation.
   * @param identifier The identifier of the table that was involved in the failed operation.
   * @param exception The exception that was thrown during the table operation, indicating the cause
   *     of the failure.
   */
  protected TableFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
