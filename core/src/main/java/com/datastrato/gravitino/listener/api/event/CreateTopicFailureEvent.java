/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.TopicInfo;

/**
 * Represents an event that is generated when an attempt to create a topic fails due to an
 * exception.
 */
@DeveloperApi
public final class CreateTopicFailureEvent extends TopicFailureEvent {
  private final TopicInfo createTopicRequest;

  /**
   * Constructs a {@code CreateTopicFailureEvent} instance, capturing detailed information about the
   * failed topic creation attempt.
   *
   * @param user The user who initiated the topic creation operation.
   * @param identifier The identifier of the topic that was attempted to be created.
   * @param exception The exception that was thrown during the topic creation operation, providing
   *     insight into what went wrong.
   * @param createTopicRequest The original request information used to attempt to create the topic.
   *     This includes details such as the intended topic schema, properties, and other
   *     configuration options that were specified.
   */
  public CreateTopicFailureEvent(
      String user, NameIdentifier identifier, Exception exception, TopicInfo createTopicRequest) {
    super(user, identifier, exception);
    this.createTopicRequest = createTopicRequest;
  }

  /**
   * Retrieves the original request information for the attempted topic creation.
   *
   * @return The {@link TopicInfo} instance representing the request information for the failed
   *     topic creation attempt.
   */
  public TopicInfo createTopicRequest() {
    return createTopicRequest;
  }
}
