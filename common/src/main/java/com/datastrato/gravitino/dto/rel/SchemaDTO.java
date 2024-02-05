/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel;

import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.rel.Schema;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;

/** Represents a Schema DTO (Data Transfer Object). */
public class SchemaDTO implements Schema {

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  private SchemaDTO() {}

  /**
   * Constructs a Schema DTO.
   *
   * @param name The name of the schema.
   * @param comment The comment associated with the schema.
   * @param properties The properties associated with the schema.
   * @param audit The audit information for the schema.
   */
  private SchemaDTO(String name, String comment, Map<String, String> properties, AuditDTO audit) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
    this.audit = audit;
  }

  /** @return The name of the schema. */
  @Override
  public String name() {
    return name;
  }

  /** @return The comment associated with the schema. */
  @Override
  public String comment() {
    return comment;
  }

  /** @return The properties associated with the schema. */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /** @return The audit information for the schema. */
  @Override
  public AuditDTO auditInfo() {
    return audit;
  }

  /**
   * Builder class for constructing SchemaDTO instances.
   *
   * @param <S> The type of the builder subclass.
   */
  public static class Builder<S extends Builder> {
    /** The name of the schema. */
    protected String name;
    /** The comment associated with the schema. */
    protected String comment;
    /** The properties associated with the schema. */
    protected Map<String, String> properties;
    /** The audit information for the schema. */
    protected AuditDTO audit;

    /** Constructs a new Builder instance. */
    public Builder() {}

    /**
     * Sets the name of the schema.
     *
     * @param name The name of the schema.
     * @return The Builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the comment associated with the schema.
     *
     * @param comment The comment associated with the schema.
     * @return The Builder instance.
     */
    public S withComment(String comment) {
      this.comment = comment;
      return (S) this;
    }

    /**
     * Sets the properties associated with the schema.
     *
     * @param properties The properties associated with the schema.
     * @return The Builder instance.
     */
    public S withProperties(Map<String, String> properties) {
      this.properties = properties;
      return (S) this;
    }

    /**
     * Sets the audit information for the schema.
     *
     * @param audit The audit information for the schema.
     * @return The Builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    /**
     * Builds a Schema DTO based on the provided builder parameters.
     *
     * @return A new SchemaDTO instance.
     * @throws IllegalArgumentException If required fields name and audit are not set.
     */
    public SchemaDTO build() {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");

      return new SchemaDTO(name, comment, properties, audit);
    }
  }
}
