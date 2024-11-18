package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represents an event triggered before loading a topic. */
@DeveloperApi
public class LoadTopicPreEvent extends TopicPreEvent {
  public LoadTopicPreEvent(String user, NameIdentifier identifier) {
    super(user, identifier);
  }
}
