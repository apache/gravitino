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
package org.apache.gravitino.dto.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.model.Model;

/** Represents a model DTO (Data Transfer Object). */
@NoArgsConstructor(access = AccessLevel.PRIVATE, force = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
public class ModelDTO implements Model {

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("latestVersion")
  private int latestVersion;

  @JsonProperty("audit")
  private AuditDTO audit;

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
  public int latestVersion() {
    return latestVersion;
  }

  @Override
  public AuditDTO auditInfo() {
    return audit;
  }

  /**
   * Creates a new builder for constructing a Model DTO.
   *
   * @return The builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for constructing a Model DTO. */
  public static class Builder {
    private String name;
    private String comment;
    private Map<String, String> properties;
    private int latestVersion;
    private AuditDTO audit;

    /**
     * Sets the name of the model.
     *
     * @param name The name of the model.
     * @return The builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the comment associated with the model.
     *
     * @param comment The comment associated with the model.
     * @return The builder.
     */
    public Builder withComment(String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Sets the properties associated with the model.
     *
     * @param properties The properties associated with the model.
     * @return The builder.
     */
    public Builder withProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    /**
     * Sets the latest version of the model.
     *
     * @param latestVersion The latest version of the model.
     * @return The builder.
     */
    public Builder withLatestVersion(int latestVersion) {
      this.latestVersion = latestVersion;
      return this;
    }

    /**
     * Sets the audit information associated with the model.
     *
     * @param audit The audit information associated with the model.
     * @return The builder.
     */
    public Builder withAudit(AuditDTO audit) {
      this.audit = audit;
      return this;
    }

    /**
     * Builds the model DTO.
     *
     * @return The model DTO.
     */
    public ModelDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(latestVersion >= 0, "latestVersion cannot be negative");
      Preconditions.checkArgument(audit != null, "audit cannot be null");

      return new ModelDTO(name, comment, properties, latestVersion, audit);
    }
  }
}
