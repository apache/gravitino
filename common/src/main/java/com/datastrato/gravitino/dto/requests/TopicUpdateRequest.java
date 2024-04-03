/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.messaging.TopicChange;
import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Represents a request to update a topic. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = TopicUpdateRequest.UpdateTopicCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = TopicUpdateRequest.SetTopicPropertyRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = TopicUpdateRequest.RemoveTopicPropertyRequest.class,
      name = "removeProperty")
})
public interface TopicUpdateRequest extends RESTRequest {

  /** @return The topic change. */
  TopicChange topicChange();

  /** Represents a request to update the comment of a topic. */
  @EqualsAndHashCode
  @ToString
  @Getter
  class UpdateTopicCommentRequest implements TopicUpdateRequest {

    @JsonProperty("newComment")
    private final String newComment;

    /**
     * Constructor for UpdateTopicCommentRequest.
     *
     * @param newComment the new comment of the topic
     */
    public UpdateTopicCommentRequest(String newComment) {
      this.newComment = newComment;
    }

    /** Default constructor for Jackson deserialization. */
    public UpdateTopicCommentRequest() {
      this(null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newComment),
          "\"newComment\" field is required and cannot be empty");
    }

    /**
     * Returns the topic change.
     *
     * @return An instance of TopicChange.
     */
    @Override
    public TopicChange topicChange() {
      return TopicChange.updateComment(newComment);
    }
  }

  /** Represents a request to set a property of a Topic. */
  @EqualsAndHashCode
  @ToString
  @Getter
  class SetTopicPropertyRequest implements TopicUpdateRequest {

    @JsonProperty("property")
    private final String property;

    @JsonProperty("value")
    private final String value;

    /**
     * Constructor for SetTopicPropertyRequest.
     *
     * @param property the property to set
     * @param value the value to set
     */
    public SetTopicPropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /** Default constructor for Jackson deserialization. */
    public SetTopicPropertyRequest() {
      this(null, null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(value != null, "\"value\" field is required and cannot be null");
    }

    /**
     * Returns the topic change.
     *
     * @return An instance of TopicChange.
     */
    @Override
    public TopicChange topicChange() {
      return TopicChange.setProperty(property, value);
    }
  }

  /** Represents a request to remove a property of a topic. */
  @EqualsAndHashCode
  @ToString
  @Getter
  class RemoveTopicPropertyRequest implements TopicUpdateRequest {

    @JsonProperty("property")
    private final String property;

    /**
     * Constructor for RemoveTopicPropertyRequest.
     *
     * @param property the property to remove
     */
    public RemoveTopicPropertyRequest(String property) {
      this.property = property;
    }

    /** Default constructor for Jackson deserialization. */
    public RemoveTopicPropertyRequest() {
      this(null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
    }

    /** @return An instance of TopicChange. */
    @Override
    public TopicChange topicChange() {
      return TopicChange.removeProperty(property);
    }
  }
}
