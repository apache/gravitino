/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.expressions;

import com.datastrato.gravitino.rel.expressions.NamedReference;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class FieldReferenceDTO implements FunctionArg, NamedReference {

  public static FieldReferenceDTO of(String... fieldName) {
    return new FieldReferenceDTO(fieldName);
  }

  private final String[] fieldName;

  private FieldReferenceDTO(String[] fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public String[] fieldName() {
    return fieldName;
  }

  @Override
  public ArgType argType() {
    return ArgType.FIELD;
  }

  public static class Builder {
    private String[] fieldName;

    public Builder withFieldName(String[] fieldName) {
      this.fieldName = fieldName;
      return this;
    }

    public Builder withColumnName(String columnName) {
      this.fieldName = new String[] {columnName};
      return this;
    }

    public FieldReferenceDTO build() {
      return new FieldReferenceDTO(fieldName);
    }
  }
}
