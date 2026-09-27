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
package org.apache.gravitino.dto.semantic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelDefinition;

/** DTO for a schema-scoped Semantic Model. */
@EqualsAndHashCode
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"name", "comment", "definition", "properties", "audit"})
public class SemanticModelDTO implements SemanticModel {

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @JsonProperty("definition")
  private SemanticModelDefinitionDTO definition;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  private SemanticModelDTO() {}

  private SemanticModelDTO(
      String name,
      @Nullable String comment,
      SemanticModelDefinitionDTO definition,
      @Nullable Map<String, String> properties,
      AuditDTO audit) {
    this.name = name;
    this.comment = comment;
    this.definition = definition;
    this.properties = immutableProperties(properties);
    this.audit = audit;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  @Nullable
  public String comment() {
    return comment;
  }

  @Override
  public SemanticModelDefinition definition() {
    Preconditions.checkArgument(definition != null, "definition must not be null");
    return definition.toDefinition();
  }

  @Override
  public Map<String, String> properties() {
    return immutableProperties(properties);
  }

  @Override
  public AuditDTO auditInfo() {
    return audit;
  }

  /**
   * Creates a builder for a Semantic Model DTO.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link SemanticModelDTO}. */
  public static final class Builder {

    private String name;
    @Nullable private String comment;
    private SemanticModelDefinitionDTO definition;
    @Nullable private Map<String, String> properties;
    private AuditDTO audit;

    private Builder() {}

    /**
     * Sets the Semantic Model name.
     *
     * @param name The Semantic Model name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the Semantic Model comment.
     *
     * @param comment The comment, or {@code null} if it is not set.
     * @return This builder.
     */
    public Builder withComment(@Nullable String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Sets the Semantic Model definition.
     *
     * @param definition The complete definition DTO.
     * @return This builder.
     */
    public Builder withDefinition(SemanticModelDefinitionDTO definition) {
      this.definition = definition;
      return this;
    }

    /**
     * Sets the Gravitino-specific Semantic Model properties.
     *
     * @param properties The properties, or {@code null} if none are set.
     * @return This builder.
     */
    public Builder withProperties(@Nullable Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    /**
     * Sets the Semantic Model audit information.
     *
     * @param audit The audit information.
     * @return This builder.
     */
    public Builder withAudit(AuditDTO audit) {
      this.audit = audit;
      return this;
    }

    /**
     * Builds a Semantic Model DTO.
     *
     * @return The Semantic Model DTO.
     * @throws IllegalArgumentException If a required field or nested DTO is invalid.
     */
    public SemanticModelDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(definition != null, "definition cannot be null");
      Preconditions.checkArgument(audit != null, "audit cannot be null");

      SemanticModelDefinition convertedDefinition = definition.toDefinition();
      return new SemanticModelDTO(
          name,
          comment,
          SemanticModelDefinitionDTO.fromDefinition(convertedDefinition),
          properties,
          audit);
    }
  }

  private static Map<String, String> immutableProperties(@Nullable Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(properties));
  }
}
