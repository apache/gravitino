/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Metalake;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a Metalake Data Transfer Object (DTO) that implements the Metalake interface. */
@EqualsAndHashCode
@ToString
public class MetalakeDTO implements Metalake {

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  protected MetalakeDTO() {}

  protected MetalakeDTO(
      String name, String comment, Map<String, String> properties, AuditDTO audit) {
    this.name = name;
    this.comment = comment;
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
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Audit auditInfo() {
    return audit;
  }

  /**
   * A builder class for constructing instances of MetalakeDTO.
   *
   * @param <S> The type of the builder subclass.
   */
  public static class Builder<S extends Builder> {

    protected String name;
    protected String comment;
    protected Map<String, String> properties;
    protected AuditDTO audit;

    public Builder() {}

    /**
     * Sets the name of the Metalake DTO.
     *
     * @param name The name of the Metalake DTO.
     * @return The builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the comment of the Metalake DTO.
     *
     * @param comment The comment of the Metalake DTO.
     * @return The builder instance.
     */
    public S withComment(String comment) {
      this.comment = comment;
      return (S) this;
    }

    /**
     * Sets the properties of the Metalake DTO.
     *
     * @param properties The properties of the Metalake DTO.
     * @return The builder instance.
     */
    public S withProperties(Map<String, String> properties) {
      this.properties = properties;
      return (S) this;
    }

    /**
     * Sets the audit information of the Metalake DTO.
     *
     * @param audit The audit information of the Metalake DTO.
     * @return The builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    /**
     * Builds an instance of MetalakeDTO using the builder's properties.
     *
     * @return An instance of MetalakeDTO.
     * @throws IllegalArgumentException If the name or audit are not set.
     */
    public MetalakeDTO build() {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      return new MetalakeDTO(name, comment, properties, audit);
    }
  }
}
