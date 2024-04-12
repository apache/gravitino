/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.messaging;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.messaging.Topic;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/** Represents a topic DTO (Data Transfer Object). */
public class TopicDTO implements Topic {

  /** @return a new builder for constructing a Topic DTO. */
  public static Builder builder() {
    return new Builder();
  }

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  private TopicDTO() {}

  /**
   * Constructs a Topic DTO.
   *
   * @param name The name of the topic.
   * @param comment The comment associated with the topic.
   * @param properties The properties associated with the topic.
   * @param audit The audit information for the topic.
   */
  private TopicDTO(String name, String comment, Map<String, String> properties, AuditDTO audit) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
    this.audit = audit;
  }

  @Override
  public String name() {
    return name;
  }

  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Audit auditInfo() {
    return audit;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Topic)) {
      return false;
    }
    TopicDTO topicDTO = (TopicDTO) o;
    return Objects.equals(name, topicDTO.name)
        && Objects.equals(comment, topicDTO.comment)
        && Objects.equals(properties, topicDTO.properties)
        && Objects.equals(audit, topicDTO.audit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, comment, properties, audit);
  }

  /** A builder for constructing a Topic DTO. */
  public static class Builder {
    private final TopicDTO topic;

    private Builder() {
      topic = new TopicDTO();
    }

    /**
     * Sets the name of the topic.
     *
     * @param name The name of the topic.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      topic.name = name;
      return this;
    }

    /**
     * Sets the comment associated with the topic.
     *
     * @param comment The comment associated with the topic.
     * @return The builder instance.
     */
    public Builder withComment(String comment) {
      topic.comment = comment;
      return this;
    }

    /**
     * Sets the properties associated with the topic.
     *
     * @param properties The properties associated with the topic.
     * @return The builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      topic.properties = properties;
      return this;
    }

    /**
     * Sets the audit information for the topic.
     *
     * @param audit The audit information for the topic.
     * @return The builder instance.
     */
    public Builder withAudit(AuditDTO audit) {
      topic.audit = audit;
      return this;
    }

    /** @return The constructed Topic DTO. */
    public TopicDTO build() {
      return topic;
    }
  }
}
