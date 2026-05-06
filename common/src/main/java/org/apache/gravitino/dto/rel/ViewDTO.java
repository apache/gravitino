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
package org.apache.gravitino.dto.rel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.View;

/** Represents a View DTO (Data Transfer Object). */
@JsonIgnoreProperties(ignoreUnknown = true)
@EqualsAndHashCode
@ToString
public class ViewDTO implements View {

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  @Nullable
  private String comment;

  @JsonProperty("columns")
  private ColumnDTO[] columns;

  @JsonProperty("representations")
  private RepresentationDTO[] representations;

  @JsonProperty("defaultCatalog")
  @Nullable
  private String defaultCatalog;

  @JsonProperty("defaultSchema")
  @Nullable
  private String defaultSchema;

  @JsonProperty("properties")
  @Nullable
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  private ViewDTO() {}

  private ViewDTO(
      String name,
      @Nullable String comment,
      ColumnDTO[] columns,
      RepresentationDTO[] representations,
      @Nullable String defaultCatalog,
      @Nullable String defaultSchema,
      @Nullable Map<String, String> properties,
      AuditDTO audit) {
    this.name = name;
    this.comment = comment;
    this.columns = columns;
    this.representations = representations;
    this.defaultCatalog = defaultCatalog;
    this.defaultSchema = defaultSchema;
    this.properties = properties;
    this.audit = audit;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Column[] columns() {
    return columns;
  }

  @Override
  public Representation[] representations() {
    return representations;
  }

  @Override
  public String defaultCatalog() {
    return defaultCatalog;
  }

  @Override
  public String defaultSchema() {
    return defaultSchema;
  }

  @Override
  public Map<String, String> properties() {
    return properties == null ? Collections.emptyMap() : properties;
  }

  @Override
  public AuditDTO auditInfo() {
    return audit;
  }

  /**
   * Creates a new {@link Builder} to build a {@link ViewDTO}.
   *
   * @return A new builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for constructing {@link ViewDTO} instances. */
  public static class Builder {

    private String name;
    @Nullable private String comment;
    private ColumnDTO[] columns;
    private RepresentationDTO[] representations;
    @Nullable private String defaultCatalog;
    @Nullable private String defaultSchema;
    @Nullable private Map<String, String> properties;
    private AuditDTO audit;

    private Builder() {}

    /**
     * Sets the view name.
     *
     * @param name The view name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the view comment.
     *
     * @param comment The view comment.
     * @return This builder.
     */
    public Builder withComment(@Nullable String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Sets the view columns.
     *
     * @param columns The view output columns.
     * @return This builder.
     */
    public Builder withColumns(ColumnDTO[] columns) {
      this.columns = columns;
      return this;
    }

    /**
     * Sets the view representations.
     *
     * @param representations The view representations.
     * @return This builder.
     */
    public Builder withRepresentations(RepresentationDTO[] representations) {
      this.representations = representations;
      return this;
    }

    /**
     * Sets the default catalog used to resolve unqualified identifiers in view representations.
     *
     * @param defaultCatalog The default catalog, or {@code null} if not set.
     * @return This builder.
     */
    public Builder withDefaultCatalog(@Nullable String defaultCatalog) {
      this.defaultCatalog = defaultCatalog;
      return this;
    }

    /**
     * Sets the default schema used to resolve unqualified identifiers in view representations.
     *
     * @param defaultSchema The default schema, or {@code null} if not set.
     * @return This builder.
     */
    public Builder withDefaultSchema(@Nullable String defaultSchema) {
      this.defaultSchema = defaultSchema;
      return this;
    }

    /**
     * Sets the view properties.
     *
     * @param properties The view properties.
     * @return This builder.
     */
    public Builder withProperties(@Nullable Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    /**
     * Sets the audit information for the view.
     *
     * @param audit The audit information.
     * @return This builder.
     */
    public Builder withAudit(AuditDTO audit) {
      this.audit = audit;
      return this;
    }

    /**
     * Builds a new {@link ViewDTO}.
     *
     * @return The constructed instance.
     * @throws IllegalArgumentException If required fields are missing.
     */
    public ViewDTO build() {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      return new ViewDTO(
          name,
          comment,
          columns == null ? new ColumnDTO[0] : columns,
          representations,
          defaultCatalog,
          defaultSchema,
          properties,
          audit);
    }
  }
}
