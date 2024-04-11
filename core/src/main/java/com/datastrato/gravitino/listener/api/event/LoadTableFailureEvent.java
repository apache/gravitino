/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that occurs when an attempt to load a table fails due to an exception. This
 * class extends {@link TableFailureEvent} to specifically tackle failure scenarios encountered
 * during the table loading process. It encapsulates the exception that led to the failure,
 * providing a detailed context for understanding the cause of the failure.
 */
@DeveloperApi
public final class LoadTableFailureEvent extends TableFailureEvent {
  /**
   * Constructs a {@code LoadTableFailureEvent} instance, capturing detailed information about the
   * unsuccessful attempt to load a table.
   *
   * @param user The user who initiated the table loading operation. This information is crucial for
   *     auditing purposes and diagnosing the cause of the failure.
   * @param identifier The identifier of the table that the loading attempt was made for. This
   *     detail assists in identifying the specific table related to the failure.
   * @param exception The exception that was thrown during the table loading operation, offering
   *     insight into the issues encountered.
   */
  public LoadTableFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
