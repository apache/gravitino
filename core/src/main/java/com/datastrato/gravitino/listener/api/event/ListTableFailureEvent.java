/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered when an attempt to list tables within a namespace fails due
 * to an exception. This class extends {@link TableFailureEvent} to cater specifically to scenarios
 * where the failure occurs in the context of listing tables. It encapsulates the exception that
 * precipitated the failure, providing detailed insight into the cause of the issue.
 */
@DeveloperApi
public final class ListTableFailureEvent extends TableFailureEvent {
  private final Namespace namespace;

  /**
   * Constructs a {@code ListTableFailureEvent} instance, capturing essential details about the
   * failed attempt to list tables.
   *
   * @param user The username of the individual who initiated the operation to list tables. This
   *     information is invaluable for auditing purposes and for understanding the context of the
   *     failure.
   * @param namespace The namespace for which the table listing was attempted. This detail helps
   *     pinpoint the specific area within the system where the failure occurred.
   * @param exception The exception encountered during the attempt to list tables, providing clarity
   *     on the nature of the failure and potential reasons behind it.
   */
  public ListTableFailureEvent(String user, Namespace namespace, Exception exception) {
    super(user, NameIdentifier.of(namespace.toString()), exception);
    this.namespace = namespace;
  }

  /**
   * Retrieves the namespace associated with this failure event, offering insights into the context
   * within which the attempt to list tables was made.
   *
   * @return A {@link Namespace} instance that encapsulates details of the intended namespace,
   *     shedding light on the scope of the operation that encountered the failure.
   */
  public Namespace namespace() {
    return namespace;
  }
}
