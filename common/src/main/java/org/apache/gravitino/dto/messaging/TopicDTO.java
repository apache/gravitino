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
package org.apache.gravitino.dto.messaging;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.messaging.Topic;

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
    if (!(o instanceof TopicDTO)) {
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

  @Override
  public String toString() {
    return "TopicDTO{"
        + "name='"
        + name
        + '\''
        + ", comment='"
        + comment
        + '\''
        + ", properties="
        + properties
        + ", audit="
        + audit
        + '}';
  }

  /** A builder for constructing a Topic DTO. */
  public static class Builder {
    private String name;
    private String comment;
    private Map<String, String> properties;
    private AuditDTO audit;

    private Builder() {}

    /**
     * Sets the name of the topic.
     *
     * @param name The name of the topic.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the comment associated with the topic.
     *
     * @param comment The comment associated with the topic.
     * @return The builder instance.
     */
    public Builder withComment(String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Sets the properties associated with the topic.
     *
     * @param properties The properties associated with the topic.
     * @return The builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    /**
     * Sets the audit information for the topic.
     *
     * @param audit The audit information for the topic.
     * @return The builder instance.
     */
    public Builder withAudit(AuditDTO audit) {
      this.audit = audit;
      return this;
    }

    /** @return The constructed Topic DTO. */
    public TopicDTO build() {
      return new TopicDTO(name, comment, properties, audit);
    }
  }
}
