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
package org.apache.gravitino.dto;

import static org.apache.gravitino.Catalog.Type.UNSUPPORTED;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;

/** Data transfer object representing catalog information. */
@EqualsAndHashCode
@ToString
public class CatalogDTO implements Catalog {

  @JsonProperty("name")
  private String name;

  @JsonProperty("type")
  private Type type;

  @JsonProperty("provider")
  private String provider;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  /** Default constructor for Jackson. */
  protected CatalogDTO() {}

  /**
   * Constructor for the catalog.
   *
   * @param name The name of the catalog.
   * @param type The type of the catalog.
   * @param provider The provider of the catalog.
   * @param comment The comment of the catalog.
   * @param properties The properties of the catalog.
   * @param audit The audit information of the catalog.
   */
  protected CatalogDTO(
      String name,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      AuditDTO audit) {
    this.name = name;
    this.type = type;
    this.provider = provider;
    this.comment = comment;
    this.properties = properties;
    this.audit = audit;
  }

  /**
   * Get the name of the catalog.
   *
   * @return The name of the catalog.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Get the type of the catalog.
   *
   * @return The type of the catalog.
   */
  @Override
  public Type type() {
    if (type == null) {
      return UNSUPPORTED;
    }
    return type;
  }

  /**
   * Get the provider of the catalog.
   *
   * @return The provider of the catalog.
   */
  @Override
  public String provider() {
    return provider;
  }

  /**
   * Get the comment of the catalog.
   *
   * @return The comment of the catalog.
   */
  @Override
  public String comment() {
    return comment;
  }

  /**
   * Get the properties of the catalog.
   *
   * @return The properties of the catalog.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Get the audit information of the catalog.
   *
   * @return The audit information of the catalog.
   */
  @Override
  public Audit auditInfo() {
    return audit;
  }

  /**
   * Builder class for constructing a CatalogDTO instance.
   *
   * @param <S> The type of the builder instance.
   */
  public static class Builder<S extends Builder> {

    /** The name of the catalog. */
    protected String name;

    /** The type of the catalog. */
    protected Type type;

    /** The provider of the catalog. */
    protected String provider;

    /** The comment of the catalog. */
    protected String comment;

    /** The properties of the catalog. */
    protected Map<String, String> properties;

    /** The audit information of the catalog. */
    protected AuditDTO audit;

    /** Default constructor for the builder. */
    protected Builder() {}

    /**
     * Sets the name of the catalog.
     *
     * @param name The name of the catalog.
     * @return The builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the type of the catalog.
     *
     * @param type The type of the catalog.
     * @return The builder instance.
     */
    public S withType(Type type) {
      this.type = type;
      return (S) this;
    }

    /**
     * Sets the provider of the catalog.
     *
     * @param provider The provider of the catalog.
     * @return The builder instance.
     */
    public S withProvider(String provider) {
      this.provider = provider;
      return (S) this;
    }

    /**
     * Sets the comment of the catalog.
     *
     * @param comment The comment of the catalog.
     * @return The builder instance.
     */
    public S withComment(String comment) {
      this.comment = comment;
      return (S) this;
    }

    /**
     * Sets the properties of the catalog.
     *
     * @param properties The properties of the catalog.
     * @return The builder instance.
     */
    public S withProperties(Map<String, String> properties) {
      this.properties = properties;
      return (S) this;
    }

    /**
     * Sets the audit information of the catalog.
     *
     * @param audit The audit information of the catalog.
     * @return The builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    /**
     * Builds an instance of CatalogDTO using the builder's properties.
     *
     * @return An instance of CatalogDTO.
     * @throws IllegalArgumentException If name, type or audit are not set.
     */
    public CatalogDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(type != null, "type cannot be null");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(provider), "provider cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");

      return new CatalogDTO(name, type, provider, comment, properties, audit);
    }
  }

  /** @return the builder for creating a new instance of CatalogDTO. */
  public static Builder builder() {
    return new Builder();
  }
}
