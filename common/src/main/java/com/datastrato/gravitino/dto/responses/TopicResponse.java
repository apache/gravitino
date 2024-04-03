/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.messaging.TopicDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Represents a response to a topic. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class TopicResponse extends BaseResponse {

  @JsonProperty("topic")
  private final TopicDTO topic;

  /**
   * Creates a new TopicResponse.
   *
   * @param topic The topic DTO object.
   */
  public TopicResponse(TopicDTO topic) {
    super(0);
    this.topic = topic;
  }

  /** This is the constructor that is used by Jackson deserializer */
  public TopicResponse() {
    super();
    this.topic = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(topic != null, "topic must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(topic.name()), "topic 'name' must not be null and empty");
  }
}
