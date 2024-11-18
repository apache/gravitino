package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.TopicInfo;

/** Represents an event triggered before creating a topic. */
@DeveloperApi
public class CreateTopicPreEvent extends TopicPreEvent {
  private final TopicInfo createTopicRequest;

  public CreateTopicPreEvent(String user, NameIdentifier identifier, TopicInfo createTopicRequest) {
    super(user, identifier);
    this.createTopicRequest = createTopicRequest;
  }

  /**
   * Retrieves the creation topic request.
   *
   * @return A {@link TopicInfo} instance encapsulating the comprehensive details of create topic
   *     request.
   */
  public TopicInfo createTopicRequest() {
    return createTopicRequest;
  }
}
