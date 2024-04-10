/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.connector.BaseTopic;

public class TestTopic extends BaseTopic {

  public static class Builder extends BaseTopicBuilder<Builder, TestTopic> {
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    @Override
    protected TestTopic internalBuild() {
      TestTopic topic = new TestTopic();
      topic.name = name;
      topic.comment = comment;
      topic.properties = properties;
      topic.auditInfo = auditInfo;
      return topic;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
