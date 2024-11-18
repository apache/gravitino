package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represents an event triggered before dropping a topic. */
@DeveloperApi
public class DropTopicPreEvent extends TopicPreEvent {
  public DropTopicPreEvent(String user, NameIdentifier identifier) {
    super(user, identifier);
  }
}
