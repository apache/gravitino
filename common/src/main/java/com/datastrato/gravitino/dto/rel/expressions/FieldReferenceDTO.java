/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.expressions;

import com.datastrato.gravitino.rel.expressions.NamedReference;
import lombok.EqualsAndHashCode;

/** Data transfer object representing a field reference. */
@EqualsAndHashCode
public class FieldReferenceDTO implements FunctionArg, NamedReference {

  /**
   * Creates a new instance of {@link FieldReferenceDTO}.
   *
   * @param fieldName The field name.
   * @return The new instance.
   */
  public static FieldReferenceDTO of(String... fieldName) {
    return new FieldReferenceDTO(fieldName);
  }

  private final String[] fieldName;

  private FieldReferenceDTO(String[] fieldName) {
    this.fieldName = fieldName;
  }

  /** @return The field name. */
  @Override
  public String[] fieldName() {
    return fieldName;
  }

  /** @return The name of the field reference. */
  @Override
  public ArgType argType() {
    return ArgType.FIELD;
  }

  /** Builder for {@link FieldReferenceDTO}. */
  public static class Builder {
    private String[] fieldName;

    /**
     * Set the field name for the field reference.
     *
     * @param fieldName The field name.
     * @return The builder.
     */
    public Builder withFieldName(String[] fieldName) {
      this.fieldName = fieldName;
      return this;
    }

    /**
     * Set the column name for the field reference.
     *
     * @param columnName The column name.
     * @return The builder.
     */
    public Builder withColumnName(String columnName) {
      this.fieldName = new String[] {columnName};
      return this;
    }

    /**
     * Build the field reference.
     *
     * @return The field reference.
     */
    public FieldReferenceDTO build() {
      return new FieldReferenceDTO(fieldName);
    }
  }
}
