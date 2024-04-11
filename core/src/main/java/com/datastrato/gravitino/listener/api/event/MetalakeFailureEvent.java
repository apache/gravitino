/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * An abstract class representing events that are triggered when a Metalake operation fails due to
 * an exception. This class extends {@link FailureEvent} to provide a more specific context related
 * to Metalake operations, encapsulating details about the user who initiated the operation, the
 * identifier of the Metalake involved, and the exception that led to the failure.
 */
@DeveloperApi
public abstract class MetalakeFailureEvent extends FailureEvent {
  /**
   * Constructs a new {@code MetalakeFailureEvent} instance, capturing information about the failed
   * Metalake operation.
   *
   * @param user The user associated with the failed Metalake operation.
   * @param identifier The identifier of the Metalake that was involved in the failed operation.
   * @param exception The exception that was thrown during the Metalake operation, indicating the
   *     cause of the failure.
   */
  protected MetalakeFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
