/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated after a topic is successfully dropped from the database.
 */
@DeveloperApi
public final class DropTopicEvent extends TopicEvent {
  private final boolean isExists;

  /**
   * Constructs a new {@code DropTopicEvent} instance, encapsulating information about the outcome
   * of a topic drop operation.
   *
   * @param user The user who initiated the drop topic operation.
   * @param identifier The identifier of the topic that was attempted to be dropped.
   * @param isExists A boolean flag indicating whether the topic existed at the time of the drop
   *     operation.
   */
  public DropTopicEvent(String user, NameIdentifier identifier, boolean isExists) {
    super(user, identifier);
    this.isExists = isExists;
  }

  /**
   * Retrieves the existence status of the topic at the time of the drop operation.
   *
   * @return A boolean value indicating whether the topic existed. {@code true} if the topic
   *     existed, otherwise {@code false}.
   */
  public boolean isExists() {
    return isExists;
  }
}
