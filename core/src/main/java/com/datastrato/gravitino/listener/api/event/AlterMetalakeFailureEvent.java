/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered when an attempt to alter a metalake fails due to an
 * exception.
 */
@DeveloperApi
public final class AlterMetalakeFailureEvent extends MetalakeFailureEvent {
  private final MetalakeChange[] metalakeChanges;

  /**
   * Constructs an {@code AlterMetalakeFailureEvent} instance, capturing detailed information about
   * the failed metalake alteration attempt.
   *
   * @param user The user who initiated the metalake alteration operation.
   * @param identifier The identifier of the metalake that was attempted to be altered.
   * @param exception The exception that was thrown during the metalake alteration operation.
   * @param metalakeChanges The changes that were attempted on the metalake.
   */
  public AlterMetalakeFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      MetalakeChange[] metalakeChanges) {
    super(user, identifier, exception);
    this.metalakeChanges = metalakeChanges.clone();
  }

  /**
   * Retrieves the changes that were attempted on the metalake.
   *
   * @return An array of {@link MetalakeChange} objects representing the attempted modifications to
   *     the metalake.
   */
  public MetalakeChange[] metalakeChanges() {
    return metalakeChanges;
  }
}
