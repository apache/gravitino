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
import org.apache.gravitino.Audit;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.model.ModelVersion;

/** Represents a model version DTO (Data Transfer Object). */
@NoArgsConstructor(access = AccessLevel.PRIVATE, force = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
public class ModelVersionDTO implements ModelVersion {

  @JsonProperty("version")
  private int version;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("aliases")
  private String[] aliases;

  @JsonProperty("uri")
  private String uri;

  @JsonProperty("uris")
  private Map<String, String> uris;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  @Override
  public Audit auditInfo() {
    return audit;
  }

  @Override
  public int version() {
    return version;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public String[] aliases() {
    return aliases;
  }

  @Override
  public Map<String, String> uris() {
    return uris;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Creates a new builder for constructing a Model Version DTO.
   *
   * @return The builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for constructing a Model Version DTO. */
  public static class Builder {
    private int version;
    private String comment;
    private String[] aliases;
    private Map<String, String> uris;
    private Map<String, String> properties;
    private AuditDTO audit;

    /**
     * Sets the version number of the model version.
     *
     * @param version The version number.
     * @return The builder.
     */
    public Builder withVersion(int version) {
      this.version = version;
      return this;
    }

    /**
     * Sets the comment of the model version.
     *
     * @param comment The comment.
     * @return The builder.
     */
    public Builder withComment(String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Sets the aliases of the model version.
     *
     * @param aliases The aliases.
     * @return The builder.
     */
    public Builder withAliases(String[] aliases) {
      this.aliases = aliases;
      return this;
    }

    /**
     * Sets the URIs of the model version.
     *
     * @param uris The URIs.
     * @return The builder.
     */
    public Builder withUris(Map<String, String> uris) {
      this.uris = uris;
      return this;
    }

    /**
     * Sets the properties of the model version.
     *
     * @param properties The properties.
     * @return The builder.
     */
    public Builder withProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    /**
     * Sets the audit information of the model version.
     *
     * @param audit The audit information.
     * @return The builder.
     */
    public Builder withAudit(AuditDTO audit) {
      this.audit = audit;
      return this;
    }

    /**
     * Builds the Model Version DTO.
     *
     * @return The Model Version DTO.
     */
    public ModelVersionDTO build() {
      Preconditions.checkArgument(version >= 0, "Version must be non-negative");
      Preconditions.checkArgument(audit != null, "Audit cannot be null");
      Preconditions.checkArgument(
          uris != null && !uris.isEmpty(),
          "At least one URI needs to be set for the model version");
      uris.forEach(
          (n, u) -> {
            Preconditions.checkArgument(StringUtils.isNotBlank(n), "URI name must not be blank");
            Preconditions.checkArgument(StringUtils.isNotBlank(u), "URI must not be blank");
          });

      return new ModelVersionDTO(
          version,
          comment,
          aliases,
          uris.get(ModelVersion.URI_NAME_UNKNOWN),
          uris,
          properties,
          audit);
    }
  }
}
