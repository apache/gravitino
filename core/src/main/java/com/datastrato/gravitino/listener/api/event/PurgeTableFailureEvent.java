/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event triggered when an attempt to purge a table from the database fails due to an
 * exception.
 */
@DeveloperApi
public final class PurgeTableFailureEvent extends TableFailureEvent {
  /**
   * Constructs a new {@code PurgeTableFailureEvent} instance.
   *
   * @param user The user who initiated the table purge operation.
   * @param identifier The identifier of the table intended to be purged.
   * @param exception The exception encountered during the table purge operation, providing insights
   *     into the reasons behind the operation's failure.
   */
  public PurgeTableFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
