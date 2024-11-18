package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.messaging.TopicChange;

/** Represents an event triggered before altering a topic. */
@DeveloperApi
public class AlterTopicPreEvent extends TopicPreEvent {
  private final TopicChange[] topicChanges;

  public AlterTopicPreEvent(String user, NameIdentifier identifier, TopicChange[] topicChanges) {
    super(user, identifier);
    this.topicChanges = topicChanges;
  }

  /**
   * Retrieves the specific changes made to the topic during the alteration process.
   *
   * @return An array of {@link TopicChange} objects detailing each modification applied to the
   *     topic.
   */
  public TopicChange[] topicChanges() {
    return topicChanges;
  }
}
