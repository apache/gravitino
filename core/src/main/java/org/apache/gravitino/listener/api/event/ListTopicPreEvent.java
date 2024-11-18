package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represents an event triggered before listing of topics within a namespace. */
@DeveloperApi
public class ListTopicPreEvent extends TopicPreEvent {
  private final Namespace namespace;

  public ListTopicPreEvent(String user, Namespace namespace) {
    super(user, NameIdentifier.of(namespace.levels()));
    this.namespace = namespace;
  }

  /**
   * Provides the namespace associated with this event.
   *
   * @return A {@link Namespace} instance from which topics were listed.
   */
  public Namespace namespace() {
    return namespace;
  }
}
