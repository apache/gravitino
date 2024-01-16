/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel;

import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.types.Type;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a Column DTO (Data Transfer Object). */
@EqualsAndHashCode
@ToString
public class ColumnDTO implements Column {

  @JsonProperty("name")
  private String name;

  @JsonProperty("type")
  @JsonSerialize(using = JsonUtils.TypeSerializer.class)
  @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
  private Type dataType;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("nullable")
  private boolean nullable = true;

  @JsonProperty("autoIncrement")
  private boolean autoIncrement = false;

  private ColumnDTO() {}

  /**
   * Constructs a Column DTO.
   *
   * @param name The name of the column.
   * @param dataType The data type of the column.
   * @param comment The comment associated with the column.
   * @param nullable Whether the column value can be null.
   * @param autoIncrement Whether the column is an auto-increment column.
   */
  private ColumnDTO(
      String name, Type dataType, String comment, boolean nullable, boolean autoIncrement) {
    this.name = name;
    this.dataType = dataType;
    this.comment = comment;
    this.nullable = nullable;
    this.autoIncrement = autoIncrement;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Type dataType() {
    return dataType;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public boolean nullable() {
    return nullable;
  }

  @Override
  public boolean autoIncrement() {
    return autoIncrement;
  }

  @Override
  public Expression defaultValue() {
    throw new UnsupportedOperationException("Column default value is not supported yet.");
  }

  /**
   * Creates a new Builder to build a Column DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing ColumnDTO instances.
   *
   * @param <S> The type of the builder subclass.
   */
  public static class Builder<S extends Builder> {

    protected String name;
    protected Type dataType;
    protected String comment;
    protected boolean nullable = true;
    protected boolean autoIncrement = false;

    public Builder() {}

    /**
     * Sets the name of the column.
     *
     * @param name The name of the column.
     * @return The Builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the data type of the column.
     *
     * @param dataType The data type of the column.
     * @return The Builder instance.
     */
    public S withDataType(Type dataType) {
      this.dataType = dataType;
      return (S) this;
    }

    /**
     * Sets the comment associated with the column.
     *
     * @param comment The comment associated with the column.
     * @return The Builder instance.
     */
    public S withComment(String comment) {
      this.comment = comment;
      return (S) this;
    }

    /**
     * Sets whether the column value can be null.
     *
     * @param nullable Whether the column value can be null.
     * @return The Builder instance.
     */
    public S withNullable(boolean nullable) {
      this.nullable = nullable;
      return (S) this;
    }

    /**
     * Sets whether the column is an auto-increment column.
     *
     * @param autoIncrement Whether the column is an auto-increment column.
     * @return The Builder instance.
     */
    public S withAutoIncrement(boolean autoIncrement) {
      this.autoIncrement = autoIncrement;
      return (S) this;
    }

    /**
     * Builds a Column DTO based on the provided builder parameters.
     *
     * @return A new ColumnDTO instance.
     * @throws NullPointerException If required fields name and data type are not set.
     */
    public ColumnDTO build() {
      Preconditions.checkNotNull(name, "Column name cannot be null");
      Preconditions.checkNotNull(dataType, "Column data type cannot be null");
      return new ColumnDTO(name, dataType, comment, nullable, autoIncrement);
    }
  }

  public void validate() throws IllegalArgumentException {
    if (autoIncrement()) {
      // TODO This part of the code will be deleted after underlying support.
      throw new UnsupportedOperationException("Auto-increment column is not supported yet.");
    }
    if (name() == null || name().isEmpty()) {
      throw new IllegalArgumentException("Column name cannot be null or empty.");
    }
    if (dataType() == null) {
      throw new IllegalArgumentException("Column data type cannot be null.");
    }
  }
}
