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
package org.apache.gravitino.dto.tag;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.tag.Tag;

/** Represents a Tag Data Transfer Object (DTO). */
public class TagDTO implements Tag {

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  @JsonProperty("inherited")
  private Optional<Boolean> inherited = Optional.empty();

  private TagDTO() {}

  @Override
  public String name() {
    return name;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public AuditDTO auditInfo() {
    return audit;
  }

  @Override
  public Optional<Boolean> inherited() {
    return inherited;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TagDTO)) {
      return false;
    }

    TagDTO tagDTO = (TagDTO) o;
    return Objects.equal(name, tagDTO.name)
        && Objects.equal(comment, tagDTO.comment)
        && Objects.equal(properties, tagDTO.properties)
        && Objects.equal(audit, tagDTO.audit);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, comment, properties, audit);
  }

  /**
   * @return a new builder for constructing a Tag DTO.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for constructing TagDTO instances. */
  public static class Builder {
    private final TagDTO tagDTO;

    private Builder() {
      tagDTO = new TagDTO();
    }

    /**
     * Sets the name of the tag.
     *
     * @param name The name of the tag.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      tagDTO.name = name;
      return this;
    }

    /**
     * Sets the comment associated with the tag.
     *
     * @param comment The comment associated with the tag.
     * @return The builder instance.
     */
    public Builder withComment(String comment) {
      tagDTO.comment = comment;
      return this;
    }

    /**
     * Sets the properties associated with the tag.
     *
     * @param properties The properties associated with the tag.
     * @return The builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      tagDTO.properties = properties;
      return this;
    }

    /**
     * Sets the audit information for the tag.
     *
     * @param audit The audit information for the tag.
     * @return The builder instance.
     */
    public Builder withAudit(AuditDTO audit) {
      tagDTO.audit = audit;
      return this;
    }

    /**
     * Sets whether the tag is inherited.
     *
     * @param inherited Whether the tag is inherited.
     * @return The builder instance.
     */
    public Builder withInherited(Optional<Boolean> inherited) {
      tagDTO.inherited = inherited;
      return this;
    }

    /**
     * @return The constructed Tag DTO.
     */
    public TagDTO build() {
      return tagDTO;
    }
  }
}
