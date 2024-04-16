/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.TopicInfo;
import com.datastrato.gravitino.messaging.TopicChange;

/** Represents an event fired when a topic is successfully altered. */
@DeveloperApi
public final class AlterTopicEvent extends TopicEvent {
  private final TopicInfo updatedTopicInfo;
  private final TopicChange[] topicChanges;

  /**
   * Constructs an instance of {@code AlterTopicEvent}, encapsulating the key details about the
   * successful alteration of a topic.
   *
   * @param user The username of the individual responsible for initiating the topic alteration.
   * @param identifier The unique identifier of the altered topic, serving as a clear reference
   *     point for the topic in question.
   * @param topicChanges An array of {@link TopicChange} objects representing the specific changes
   *     applied to the topic during the alteration process.
   * @param updatedTopicInfo The post-alteration state of the topic.
   */
  public AlterTopicEvent(
      String user,
      NameIdentifier identifier,
      TopicChange[] topicChanges,
      TopicInfo updatedTopicInfo) {
    super(user, identifier);
    this.topicChanges = topicChanges.clone();
    this.updatedTopicInfo = updatedTopicInfo;
  }

  /**
   * Retrieves the updated state of the topic after the successful alteration.
   *
   * @return A {@link TopicInfo} instance encapsulating the details of the altered topic.
   */
  public TopicInfo updatedTopicInfo() {
    return updatedTopicInfo;
  }

  /**
   * Retrieves the specific changes that were made to the topic during the alteration process.
   *
   * @return An array of {@link TopicChange} objects detailing each modification applied to the
   *     topic.
   */
  public TopicChange[] topicChanges() {
    return topicChanges;
  }
}
