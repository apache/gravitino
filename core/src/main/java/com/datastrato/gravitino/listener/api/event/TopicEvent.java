/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an abstract base class for events related to topic operations. This class extends
 * {@link Event} to provide a more specific context involving operations on topics, such as
 * creation, deletion, or modification. It captures essential information including the user
 * performing the operation and the identifier of the topic being operated on.
 *
 * <p>Concrete implementations of this class should provide additional details pertinent to the
 * specific type of topic operation being represented.
 */
@DeveloperApi
public abstract class TopicEvent extends Event {
  /**
   * Constructs a new {@code TopicEvent} with the specified user and topic identifier.
   *
   * @param user The user responsible for triggering the topic operation.
   * @param identifier The identifier of the topic involved in the operation. This encapsulates
   *     details such as the metalake, catalog, schema, and topic name.
   */
  protected TopicEvent(String user, NameIdentifier identifier) {
    super(user, identifier);
  }
}
