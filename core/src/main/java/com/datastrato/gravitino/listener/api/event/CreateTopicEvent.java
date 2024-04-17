/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.TopicInfo;

/** Represents an event triggered upon the successful creation of a topic. */
@DeveloperApi
public final class CreateTopicEvent extends TopicEvent {
  private final TopicInfo createdTopicInfo;

  /**
   * Constructs an instance of {@code CreateTopicEvent}, capturing essential details about the
   * successful creation of a topic.
   *
   * @param user The username of the individual who initiated the topic creation.
   * @param identifier The unique identifier of the topic that was created.
   * @param createdTopicInfo The final state of the topic post-creation.
   */
  public CreateTopicEvent(String user, NameIdentifier identifier, TopicInfo createdTopicInfo) {
    super(user, identifier);
    this.createdTopicInfo = createdTopicInfo;
  }

  /**
   * Retrieves the final state of the topic as it was returned to the user after successful
   * creation.
   *
   * @return A {@link TopicInfo} instance encapsulating the comprehensive details of the newly
   *     created topic.
   */
  public TopicInfo createdTopicInfo() {
    return createdTopicInfo;
  }
}
