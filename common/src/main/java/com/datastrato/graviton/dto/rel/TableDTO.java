/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.rel;

import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Table;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;

/** Represents a Table DTO (Data Transfer Object). */
public class TableDTO implements Table {

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("columns")
  private ColumnDTO[] columns;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  private TableDTO() {}

  /**
   * Constructs a Table DTO.
   *
   * @param name The name of the table.
   * @param comment The comment associated with the table.
   * @param columns The columns of the table.
   * @param properties The properties associated with the table.
   * @param audit The audit information for the table.
   */
  private TableDTO(
      String name,
      String comment,
      ColumnDTO[] columns,
      Map<String, String> properties,
      AuditDTO audit) {
    this.name = name;
    this.comment = comment;
    this.columns = columns;
    this.properties = properties;
    this.audit = audit;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Column[] columns() {
    return columns;
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

  /**
   * Creates a new Builder to build a Table DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing TableDTO instances.
   *
   * @param <S> The type of the builder subclass.
   */
  public static class Builder<S extends Builder> {
    protected String name;
    protected String comment;
    protected ColumnDTO[] columns;
    protected Map<String, String> properties;
    protected AuditDTO audit;

    public Builder() {}

    /**
     * Sets the name of the table.
     *
     * @param name The name of the table.
     * @return The Builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the comment associated with the table.
     *
     * @param comment The comment associated with the table.
     * @return The Builder instance.
     */
    public S withComment(String comment) {
      this.comment = comment;
      return (S) this;
    }

    /**
     * Sets the columns of the table.
     *
     * @param columns The columns of the table.
     * @return The Builder instance.
     */
    public S withColumns(ColumnDTO[] columns) {
      this.columns = columns;
      return (S) this;
    }

    /**
     * Sets the properties associated with the table.
     *
     * @param properties The properties associated with the table.
     * @return The Builder instance.
     */
    public S withProperties(Map<String, String> properties) {
      this.properties = properties;
      return (S) this;
    }

    /**
     * Sets the audit information for the table.
     *
     * @param audit The audit information for the table.
     * @return The Builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    /**
     * Builds a Table DTO based on the provided builder parameters.
     *
     * @return A new TableDTO instance.
     * @throws IllegalArgumentException If required fields name, columns and audit are not set.
     */
    public TableDTO build() {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "name cannot be null or empty");
      Preconditions.checkArgument(
          columns != null && columns.length > 0, "columns cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");

      return new TableDTO(name, comment, columns, properties, audit);
    }
  }
}
