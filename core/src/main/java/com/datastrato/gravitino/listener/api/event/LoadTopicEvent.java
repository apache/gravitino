/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.TopicInfo;

/** Represents an event triggered upon the successful loading of a topic. */
@DeveloperApi
public final class LoadTopicEvent extends TopicEvent {
  private final TopicInfo loadedTopicInfo;

  /**
   * Constructs an instance of {@code LoadTopicEvent}.
   *
   * @param user The username of the individual who initiated the topic loading.
   * @param identifier The unique identifier of the topic that was loaded.
   * @param topicInfo The state of the topic post-loading.
   */
  public LoadTopicEvent(String user, NameIdentifier identifier, TopicInfo topicInfo) {
    super(user, identifier);
    this.loadedTopicInfo = topicInfo;
  }

  /**
   * Retrieves the state of the topic as it was made available to the user after successful loading.
   *
   * @return A {@link TopicInfo} instance encapsulating the details of the topic as loaded.
   */
  public TopicInfo loadedTopicInfo() {
    return loadedTopicInfo;
  }
}
