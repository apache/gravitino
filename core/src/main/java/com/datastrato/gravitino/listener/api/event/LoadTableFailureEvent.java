/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/** Represents an event that occurs when an attempt to load a table fails due to an exception. */
@DeveloperApi
public final class LoadTableFailureEvent extends TableFailureEvent {
  /**
   * Constructs a {@code LoadTableFailureEvent} instance.
   *
   * @param user The user who initiated the table loading operation.
   * @param identifier The identifier of the table that the loading attempt was made for.
   * @param exception The exception that was thrown during the table loading operation, offering
   *     insight into the issues encountered.
   */
  public LoadTableFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
