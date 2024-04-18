/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered when an attempt to list topics within a namespace fails due
 * to an exception.
 */
@DeveloperApi
public final class ListTopicFailureEvent extends TopicFailureEvent {
  private final Namespace namespace;

  /**
   * Constructs a {@code ListTopicFailureEvent} instance.
   *
   * @param user The username of the individual who initiated the operation to list topics.
   * @param namespace The namespace for which the topic listing was attempted.
   * @param exception The exception encountered during the attempt to list topics.
   */
  public ListTopicFailureEvent(String user, Namespace namespace, Exception exception) {
    super(user, NameIdentifier.of(namespace.levels()), exception);
    this.namespace = namespace;
  }

  /**
   * Retrieves the namespace associated with this failure event.
   *
   * @return A {@link Namespace} instance for which the topic listing was attempted
   */
  public Namespace namespace() {
    return namespace;
  }
}
