/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated when an attempt to drop a table from the database fails due
 * to an exception. This class extends {@link TableFailureEvent} to provide specific context related
 * to table drop failures, encapsulating details about the user who initiated the operation, the
 * identifier of the table that was attempted to be dropped, and the exception that led to the
 * failure. This event can be used for auditing purposes and to facilitate error handling and
 * diagnostic processes.
 */
@DeveloperApi
public final class DropTableFailureEvent extends TableFailureEvent {
  /**
   * Constructs a new {@code DropTableFailureEvent} instance, capturing detailed information about
   * the failed attempt to drop a table.
   *
   * @param user The user who initiated the drop table operation. This information is crucial for
   *     understanding the context of the operation and for auditing who is responsible for the
   *     attempted change.
   * @param identifier The identifier of the table that the operation attempted to drop. This
   *     provides a clear reference to the specific table involved in the failure.
   * @param exception The exception that was thrown during the drop table operation, offering
   *     insights into what went wrong and why the operation failed.
   */
  public DropTableFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
