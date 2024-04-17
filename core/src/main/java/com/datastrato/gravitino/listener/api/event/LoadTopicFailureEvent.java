/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/** Represents an event that occurs when an attempt to load a topic fails due to an exception. */
@DeveloperApi
public final class LoadTopicFailureEvent extends TopicFailureEvent {
  /**
   * Constructs a {@code LoadTopicFailureEvent} instance.
   *
   * @param user The user who initiated the topic loading operation.
   * @param identifier The identifier of the topic that the loading attempt was made for.
   * @param exception The exception that was thrown during the topic loading operation, offering
   *     insight into the issues encountered.
   */
  public LoadTopicFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
