package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represents a pre-event for topic operations. */
@DeveloperApi
public abstract class TopicPreEvent extends PreEvent {
  protected TopicPreEvent(String user, NameIdentifier identifier) {
    super(user, identifier);
  }
}
