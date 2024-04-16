/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * An abstract class representing events that are triggered when a topic operation fails due to an
 * exception. This class extends {@link FailureEvent} to provide a more specific context related to
 * topic operations, encapsulating details about the user who initiated the operation, the
 * identifier of the topic involved, and the exception that led to the failure.
 *
 * <p>Implementations of this class can be used to convey detailed information about failures in
 * operations such as creating, updating, deleting, or querying topics, making it easier to diagnose
 * and respond to issues.
 */
@DeveloperApi
public abstract class TopicFailureEvent extends FailureEvent {
  /**
   * Constructs a new {@code TopicFailureEvent} instance, capturing information about the failed
   * topic operation.
   *
   * @param user The user associated with the failed topic operation.
   * @param identifier The identifier of the topic that was involved in the failed operation.
   * @param exception The exception that was thrown during the topic operation, indicating the cause
   *     of the failure.
   */
  protected TopicFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
