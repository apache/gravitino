/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.rel.transforms;

import com.datastrato.graviton.rel.Column;
import com.google.common.annotations.VisibleForTesting;
import io.substrait.expression.Expression;
import java.util.Arrays;
import lombok.EqualsAndHashCode;

/** Helper methods to create logical transforms to pass into Graviton. */
public class Transforms {

  public static final String YEAR = "year";
  public static final String MONTH = "month";
  public static final String DAY = "day";
  public static final String HOUR = "hour";
  public static final String LIST = "list";
  public static final String RANGE = "range";

  public static FieldTransform identity(String[] fieldName) {
    return field(fieldName);
  }

  public static FunctionTransform year(String[] fieldName) {
    return function(YEAR, new Transform[] {field(fieldName)});
  }

  public static FunctionTransform month(String[] fieldName) {
    return function(MONTH, new Transform[] {field(fieldName)});
  }

  public static FunctionTransform day(String[] fieldName) {
    return function(DAY, new Transform[] {field(fieldName)});
  }

  public static FunctionTransform hour(String[] fieldName) {
    return function(HOUR, new Transform[] {field(fieldName)});
  }

  public static FunctionTransform list(String[][] fieldNames) {
    Transform[] args = Arrays.stream(fieldNames).map(Transforms::field).toArray(Transform[]::new);
    return function(LIST, args);
  }

  public static FunctionTransform range(String[] fieldName) {
    return function(RANGE, new Transform[] {field(fieldName)});
  }

  public static LiteralTransform literal(Expression.Literal value) {
    return new Transforms.LiteralReference(value);
  }

  public static FieldTransform field(String[] fieldName) {
    return new Transforms.NamedReference(fieldName);
  }

  public static FieldTransform field(Column column) {
    return field(new String[] {column.name()});
  }

  public static FunctionTransform function(String name, Transform[] args) {
    return new Transforms.FunctionTrans(name, args);
  }

  @VisibleForTesting
  @EqualsAndHashCode(callSuper = false)
  public static final class NamedReference extends FieldTransform {
    private final String[] fieldName;

    public NamedReference(String[] fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public String[] value() {
      return fieldName;
    }
  }

  @VisibleForTesting
  @EqualsAndHashCode(callSuper = false)
  public static final class LiteralReference extends LiteralTransform {
    private final Expression.Literal value;

    public LiteralReference(Expression.Literal value) {
      this.value = value;
    }

    @Override
    public Expression.Literal value() {
      return value;
    }
  }

  @VisibleForTesting
  @EqualsAndHashCode(callSuper = false)
  public static final class FunctionTrans extends FunctionTransform {
    private final String name;
    private final Transform[] args;

    public FunctionTrans(String name, Transform[] args) {
      this.name = name;
      this.args = args;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Transform[] arguments() {
      return args;
    }
  }
}
