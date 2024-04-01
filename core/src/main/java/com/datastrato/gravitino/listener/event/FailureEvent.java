/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an abstract base class for events that indicate a failure in an operation. This class
 * extends {@link Event} to encapsulate common information related to failures, such as the user who
 * performed the operation, the resource identifier, and the exception that was thrown during the
 * operation. This class serves as a foundation for more specific failure event types that might
 * include additional context or details about the failure.
 */
@DeveloperApi
public abstract class FailureEvent extends Event {
  private Exception exception;

  /**
   * Constructs a new {@code FailureEvent} instance with the specified user, resource identifier,
   * and the exception that was thrown.
   *
   * @param user The user associated with the operation that resulted in a failure. This information
   *     is important for auditing and understanding the context of the failure.
   * @param identifier The identifier of the resource involved in the operation that failed. This
   *     provides a clear reference to what was being acted upon when the exception occurred.
   * @param exception The exception that was thrown during the operation. This is the primary piece
   *     of information indicating what went wrong.
   */
  protected FailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier);
    this.exception = exception;
  }

  /**
   * Retrieves the exception that was thrown during the operation, indicating the reason for the
   * failure.
   *
   * @return The exception thrown by the operation.
   */
  public Exception getException() {
    return exception;
  }
}
