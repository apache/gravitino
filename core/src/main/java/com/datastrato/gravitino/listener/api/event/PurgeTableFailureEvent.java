/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event triggered when an attempt to purge a table from the database fails due to an
 * exception. This class extends {@link TableFailureEvent} to deliver a specific context concerning
 * failures encountered during the table purge process. It encapsulates details about the user who
 * initiated the operation, the identifier of the table targeted for purging, and the exception that
 * caused the failure. This event serves critical functions in auditing, error handling, and
 * diagnostics.
 */
@DeveloperApi
public final class PurgeTableFailureEvent extends TableFailureEvent {
  /**
   * Constructs a new {@code PurgeTableFailureEvent} instance, capturing detailed information about
   * the unsuccessful attempt to purge a table.
   *
   * @param user The user who initiated the table purge operation. This detail is vital for
   *     understanding the operation's context and auditing responsibility for the attempted action.
   * @param identifier The identifier of the table intended to be purged. This offers a precise
   *     reference to the specific table implicated in the failure.
   * @param exception The exception encountered during the table purge operation, providing insights
   *     into the reasons behind the operation's failure.
   */
  public PurgeTableFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
