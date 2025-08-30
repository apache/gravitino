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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.Audit;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

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

  @JsonProperty("defaultValue")
  // the NON_EMPTY annotation is used to avoid serializing the defaultValue field if it is not set
  // or null.
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonSerialize(using = JsonUtils.ColumnDefaultValueSerializer.class)
  @JsonDeserialize(using = JsonUtils.ColumnDefaultValueDeserializer.class)
  private Expression defaultValue = Column.DEFAULT_VALUE_NOT_SET;

  private ColumnDTO() {}

  /**
   * Constructs a Column DTO.
   *
   * @param name The name of the column.
   * @param dataType The data type of the column.
   * @param comment The comment associated with the column.
   * @param nullable Whether the column value can be null.
   * @param autoIncrement Whether the column is an auto-increment column.
   * @param defaultValue The default value of the column.
   */
  private ColumnDTO(
      String name,
      Type dataType,
      String comment,
      boolean nullable,
      boolean autoIncrement,
      Expression defaultValue) {
    this.name = name;
    this.dataType = dataType;
    this.comment = comment;
    this.nullable = nullable;
    this.autoIncrement = autoIncrement;
    this.defaultValue = defaultValue == null ? Column.DEFAULT_VALUE_NOT_SET : defaultValue;
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
    return defaultValue;
  }

  @Override
  public Audit auditInfo() {
    return AuditDTO.builder().build();
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

    /** The name of the column. */
    protected String name;

    /** The data type of the column. */
    protected Type dataType;

    /** The comment associated with the column. */
    protected String comment;

    /** * Whether the column value can be null. */
    protected boolean nullable = true;

    /** Whether the column is an auto-increment column. */
    protected boolean autoIncrement = false;

    /** The default value of the column. */
    protected Expression defaultValue;

    /** Constructs a new Builder. */
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
     * Sets the default value of the column.
     *
     * @param defaultValue The default value of the column.
     * @return The Builder instance.
     */
    public S withDefaultValue(Expression defaultValue) {
      this.defaultValue = defaultValue;
      return (S) this;
    }

    /**
     * Builds a Column DTO based on the provided builder parameters.
     *
     * @return A new ColumnDTO instance.
     * @throws NullPointerException If required, fields name and data type are not set.
     */
    public ColumnDTO build() {
      Preconditions.checkNotNull(name, "Column name cannot be null");
      Preconditions.checkNotNull(dataType, "Column data type cannot be null");
      return new ColumnDTO(name, dataType, comment, nullable, autoIncrement, defaultValue);
    }
  }

  /**
   * Validates the Column DTO.
   *
   * @throws IllegalArgumentException If some of the required fields are not set or if the column is
   *     non-nullable with a null default value, this method will throw an IllegalArgumentException.
   */
  public void validate() throws IllegalArgumentException {
    if (name() == null || name().isEmpty()) {
      throw new IllegalArgumentException("Column name cannot be null or empty.");
    }
    if (dataType() == null) {
      throw new IllegalArgumentException("Column data type cannot be null.");
    }
    if (!nullable()
        && defaultValue() instanceof LiteralDTO
        && ((LiteralDTO) defaultValue()).dataType().equals(Types.NullType.get())) {
      throw new IllegalArgumentException(
          "Column cannot be non-nullable with a null default value: " + name());
    }
  }
}
