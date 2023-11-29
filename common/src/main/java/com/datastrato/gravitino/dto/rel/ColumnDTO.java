/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel;

import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rel.Column;
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
  private boolean nullable;

  private ColumnDTO() {}

  /**
   * Constructs a Column DTO.
   *
   * @param name The name of the column.
   * @param dataType The data type of the column.
   * @param comment The comment associated with the column.
   */
  private ColumnDTO(String name, Type dataType, String comment) {
    this(name, dataType, comment, true);
  }

  /**
   * Constructs a Column DTO.
   *
   * @param name The name of the column.
   * @param dataType The data type of the column.
   * @param comment The comment associated with the column.
   * @param nullable Whether the column value can be null.
   */
  private ColumnDTO(String name, Type dataType, String comment, boolean nullable) {
    this.name = name;
    this.dataType = dataType;
    this.comment = comment;
    this.nullable = nullable;
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
     * Builds a Column DTO based on the provided builder parameters.
     *
     * @return A new ColumnDTO instance.
     * @throws NullPointerException If required fields name and data type are not set.
     */
    public ColumnDTO build() {
      Preconditions.checkNotNull(name, "Column name cannot be null");
      Preconditions.checkNotNull(dataType, "Column data type cannot be null");
      return new ColumnDTO(name, dataType, comment, nullable);
    }
  }
}
