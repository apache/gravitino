/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto;

import com.datastrato.graviton.Audit;
import com.datastrato.graviton.Catalog;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Data transfer object representing catalog information. */
@EqualsAndHashCode
@ToString
public class CatalogDTO implements Catalog {

  @JsonProperty("name")
  private String name;

  @JsonProperty("type")
  private Type type;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  private CatalogDTO() {}

  private CatalogDTO(
      String name, Type type, String comment, Map<String, String> properties, AuditDTO audit) {
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.properties = properties;
    this.audit = audit;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Type type() {
    return type;
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
  public Audit auditInfo() {
    return audit;
  }

  /**
   * Creates a new Builder for constructing a Catalog DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing a CatalogDTO instance.
   *
   * @param <S> The type of the builder instance.
   */
  public static class Builder<S extends Builder> {
    protected String name;
    protected Type type;
    protected String comment;
    protected Map<String, String> properties;
    protected AuditDTO audit;

    public Builder() {}

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
    public Builder withProperties(Map<String, String> properties) {
      this.properties = properties;
      return (S) this;
    }

    /**
     * Sets the audit information of the catalog.
     *
     * @param audit The audit information of the catalog.
     * @return The builder instance.
     */
    public Builder withAudit(AuditDTO audit) {
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
      Preconditions.checkArgument(name != null && !name.isEmpty(), "name cannot be null or empty");
      Preconditions.checkArgument(type != null, "type cannot be null");
      Preconditions.checkArgument(audit != null, "audit cannot be null");

      return new CatalogDTO(name, type, comment, properties, audit);
    }
  }
}
