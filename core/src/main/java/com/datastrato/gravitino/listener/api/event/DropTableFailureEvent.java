/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated when an attempt to drop a table from the database fails due
 * to an exception.
 */
@DeveloperApi
public final class DropTableFailureEvent extends TableFailureEvent {
  /**
   * Constructs a new {@code DropTableFailureEvent} instance, capturing detailed information about
   * the failed attempt to drop a table.
   *
   * @param user The user who initiated the drop table operation.
   * @param identifier The identifier of the table that the operation attempted to drop.
   * @param exception The exception that was thrown during the drop table operation, offering
   *     insights into what went wrong and why the operation failed.
   */
  public DropTableFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
