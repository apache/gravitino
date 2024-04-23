/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.DeveloperApi;

/** Represents an event that is triggered upon the successful list of topics within a namespace. */
@DeveloperApi
public final class ListTopicEvent extends TopicEvent {
  private final Namespace namespace;

  /**
   * Constructs an instance of {@code ListTopicEvent}.
   *
   * @param user The username of the individual who initiated the topic listing.
   * @param namespace The namespace from which topics were listed.
   */
  public ListTopicEvent(String user, Namespace namespace) {
    super(user, NameIdentifier.of(namespace.levels()));
    this.namespace = namespace;
  }

  /**
   * Provides the namespace associated with this event.
   *
   * @return A {@link Namespace} instance from which topics were listed.
   */
  public Namespace namespace() {
    return namespace;
  }
}
