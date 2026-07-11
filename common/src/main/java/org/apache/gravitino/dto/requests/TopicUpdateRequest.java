/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.messaging.DataLayoutDTO;
import org.apache.gravitino.messaging.DataLayouts;
import org.apache.gravitino.messaging.TopicChange;
import org.apache.gravitino.rest.RESTRequest;

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
      name = "removeProperty"),
  @JsonSubTypes.Type(
      value = TopicUpdateRequest.UpdateTopicDataLayoutRequest.class,
      name = "updateDataLayout"),
  @JsonSubTypes.Type(
      value = TopicUpdateRequest.RemoveTopicDataLayoutRequest.class,
      name = "removeDataLayout"),
  @JsonSubTypes.Type(
      value = TopicUpdateRequest.RemoveTopicDataLayoutsRequest.class,
      name = "removeDataLayouts")
})
public interface TopicUpdateRequest extends RESTRequest {

  /**
   * @return The topic change.
   */
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

    /** Validates the fields of the request. Always pass. */
    @Override
    public void validate() throws IllegalArgumentException {}

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
      DataLayouts.validateNoReservedProperties(Collections.singletonMap(property, value));
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

    /**
     * @return An instance of TopicChange.
     */
    @Override
    public TopicChange topicChange() {
      return TopicChange.removeProperty(property);
    }
  }

  /** Represents a request to update the data layout of a topic. */
  @EqualsAndHashCode
  @ToString
  @Getter
  class UpdateTopicDataLayoutRequest implements TopicUpdateRequest {

    @JsonProperty("name")
    private final String name;

    @JsonProperty("newDataLayout")
    private final DataLayoutDTO newDataLayout;

    /**
     * Constructor for UpdateTopicDataLayoutRequest.
     *
     * @param name the layout name to update
     * @param newDataLayout the new data layout of the topic
     */
    public UpdateTopicDataLayoutRequest(String name, DataLayoutDTO newDataLayout) {
      this.name = name;
      this.newDataLayout = newDataLayout;
    }

    /** Default constructor for Jackson deserialization. */
    public UpdateTopicDataLayoutRequest() {
      this(null, null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
      Preconditions.checkArgument(newDataLayout != null, "\"newDataLayout\" field is required");
      try {
        newDataLayout.validate();
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("data layout \"" + name + "\": " + e.getMessage(), e);
      }
    }

    @Override
    public TopicChange topicChange() {
      return TopicChange.updateDataLayout(name, newDataLayout.toSchemaDataLayout());
    }
  }

  /** Represents a request to remove the data layout of a topic. */
  @EqualsAndHashCode
  @ToString
  @Getter
  class RemoveTopicDataLayoutRequest implements TopicUpdateRequest {

    @JsonProperty("name")
    private final String name;

    /**
     * Constructor for RemoveTopicDataLayoutRequest.
     *
     * @param name the layout name to remove
     */
    public RemoveTopicDataLayoutRequest(String name) {
      this.name = name;
    }

    /** Default constructor for Jackson deserialization. */
    public RemoveTopicDataLayoutRequest() {
      this(null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
    }

    @Override
    public TopicChange topicChange() {
      return TopicChange.removeDataLayout(name);
    }
  }

  /** Represents a request to remove all data layouts of a topic. */
  @EqualsAndHashCode
  @ToString
  class RemoveTopicDataLayoutsRequest implements TopicUpdateRequest {

    /** Default constructor for Jackson deserialization. */
    public RemoveTopicDataLayoutsRequest() {}

    @Override
    public void validate() throws IllegalArgumentException {}

    @Override
    public TopicChange topicChange() {
      return TopicChange.removeDataLayouts();
    }
  }
}
