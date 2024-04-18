/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.messaging.TopicChange;

/**
 * Represents an event that is triggered when an attempt to alter a topic fails due to an exception.
 */
@DeveloperApi
public final class AlterTopicFailureEvent extends TopicFailureEvent {
  private final TopicChange[] topicChanges;

  /**
   * Constructs an {@code AlterTopicFailureEvent} instance, capturing detailed information about the
   * failed topic alteration attempt.
   *
   * @param user The user who initiated the topic alteration operation.
   * @param identifier The identifier of the topic that was attempted to be altered.
   * @param exception The exception that was thrown during the topic alteration operation.
   * @param topicChanges The changes that were attempted on the topic.
   */
  public AlterTopicFailureEvent(
      String user, NameIdentifier identifier, Exception exception, TopicChange[] topicChanges) {
    super(user, identifier, exception);
    this.topicChanges = topicChanges.clone();
  }

  /**
   * Retrieves the changes that were attempted on the topic.
   *
   * @return An array of {@link TopicChange} objects representing the attempted modifications to the
   *     topic.
   */
  public TopicChange[] topicChanges() {
    return topicChanges;
  }
}
