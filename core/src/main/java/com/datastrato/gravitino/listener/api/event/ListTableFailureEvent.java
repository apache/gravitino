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
 * to an exception.
 */
@DeveloperApi
public final class ListTableFailureEvent extends TableFailureEvent {
  private final Namespace namespace;

  /**
   * Constructs a {@code ListTableFailureEvent} instance.
   *
   * @param user The username of the individual who initiated the operation to list tables.
   * @param namespace The namespace for which the table listing was attempted.
   * @param exception The exception encountered during the attempt to list tables.
   */
  public ListTableFailureEvent(String user, Namespace namespace, Exception exception) {
    super(user, NameIdentifier.of(namespace.levels()), exception);
    this.namespace = namespace;
  }

  /**
   * Retrieves the namespace associated with this failure event.
   *
   * @return A {@link Namespace} instance for which the table listing was attempted
   */
  public Namespace namespace() {
    return namespace;
  }
}
