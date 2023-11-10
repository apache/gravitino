/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions;

import lombok.EqualsAndHashCode;

public interface NamedReference extends Expression {

  static FieldReference field(String... fieldName) {
    return new FieldReference(fieldName);
  }

  /**
   * Returns the referenced field name as an array of String parts.
   *
   * <p>Each string in the returned array represents a field name.
   */
  String[] fieldName();

  @Override
  default Expression[] children() {
    return EMPTY_EXPRESSION;
  }

  @Override
  default NamedReference[] references() {
    return new NamedReference[] {this};
  }

  @EqualsAndHashCode
  final class FieldReference implements NamedReference {
    private final String[] fieldName;

    private FieldReference(String[] fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public String[] fieldName() {
      return fieldName;
    }
  }
}
