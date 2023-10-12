/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.rel.transforms;

import static io.substrait.expression.ExpressionCreator.i32;

import com.datastrato.graviton.rel.Column;
import com.google.common.annotations.VisibleForTesting;
import io.substrait.expression.Expression;
import java.util.Arrays;
import lombok.EqualsAndHashCode;

/** Helper methods to create logical transforms to pass into Graviton. */
public class Transforms {

  public static final String NAME_OF_YEAR = "year";
  public static final String NAME_OF_MONTH = "month";
  public static final String NAME_OF_DAY = "day";
  public static final String NAME_OF_HOUR = "hour";
  public static final String NAME_OF_TRUNCATE = "truncate";
  public static final String NAME_OF_BUCKET = "bucket";
  public static final String NAME_OF_LIST = "list";
  public static final String NAME_OF_RANGE = "range";

  /**
   * Creates a partitioning by unmodified source field value.
   *
   * @param fieldName The field name to partition by.
   * @return The identity partitioning.
   */
  public static NamedReference identity(String[] fieldName) {
    return field(fieldName);
  }

  /**
   * Creates a partitioning by given column's name.
   *
   * @param column The column to partition by.
   * @return The identity partitioning.
   */
  public static NamedReference identity(Column column) {
    return field(new String[] {column.name()});
  }

  /**
   * Creates a partitioning by extract a date or timestamp year value.
   *
   * @param fieldName The field name to partition by.
   * @return The year partitioning.
   */
  public static FunctionTransform year(String[] fieldName) {
    return function(NAME_OF_YEAR, new Transform[] {field(fieldName)});
  }

  /**
   * Creates a partitioning by extract a date or timestamp month value.
   *
   * @param fieldName The field name to partition by.
   * @return The month partitioning.
   */
  public static FunctionTransform month(String[] fieldName) {
    return function(NAME_OF_MONTH, new Transform[] {field(fieldName)});
  }

  /**
   * Creates a partitioning by extract a date or timestamp day value.
   *
   * @param fieldName The field name to partition by.
   * @return The day partitioning.
   */
  public static FunctionTransform day(String[] fieldName) {
    return function(NAME_OF_DAY, new Transform[] {field(fieldName)});
  }

  /**
   * Creates a partitioning by extract a timestamp hour value.
   *
   * @param fieldName The field name to partition by.
   * @return The hour partitioning.
   */
  public static FunctionTransform hour(String[] fieldName) {
    return function(NAME_OF_HOUR, new Transform[] {field(fieldName)});
  }

  /**
   * Creates a bucket partitioning by the given field name and number of buckets.
   *
   * @param fieldName The field name to partition by.
   * @param numBuckets The number of buckets.
   * @return The bucket partitioning.
   */
  public static FunctionTransform bucket(String[] fieldName, int numBuckets) {
    Transforms.LiteralReference bucketNum = Transforms.literal(i32(false, numBuckets));
    Transforms.NamedReference field = Transforms.field(fieldName);
    return function(NAME_OF_BUCKET, new Transform[] {field, bucketNum});
  }

  /**
   * Creates a truncate partitioning by the given field name and width.
   *
   * @param fieldName The field name to partition by.
   * @param width The width.
   * @return The truncate partitioning.
   */
  public static FunctionTransform truncate(String[] fieldName, int width) {
    Transforms.LiteralReference bucketNum = Transforms.literal(i32(false, width));
    Transforms.NamedReference field = Transforms.field(fieldName);
    return function(NAME_OF_TRUNCATE, new Transform[] {field, bucketNum});
  }

  /**
   * Creates a list partitioning by the given field names. For dynamically partitioned tables only.
   *
   * @param fieldNames The field names to partition by.
   * @return The list partitioning.
   */
  public static FunctionTransform list(String[][] fieldNames) {
    Transform[] args = Arrays.stream(fieldNames).map(Transforms::field).toArray(Transform[]::new);
    return function(NAME_OF_LIST, args);
  }

  /**
   * Creates a range partitioning by the given field name. For dynamically partitioned tables only.
   *
   * @param fieldName The field name to partition by.
   * @return The range partitioning.
   */
  public static FunctionTransform range(String[] fieldName) {
    return function(NAME_OF_RANGE, new Transform[] {field(fieldName)});
  }

  /**
   * Creates a literal reference, which is means constant value and is usually used as second
   * argument for expression partitioning {@link
   * com.datastrato.graviton.rel.transforms.Transforms#function(java.lang.String,
   * com.datastrato.graviton.rel.transforms.Transform[])}.
   *
   * @param value The literal value of {@link Expression.Literal} type
   * @return The literal reference.
   */
  public static LiteralReference literal(Expression.Literal value) {
    return new Transforms.LiteralReference(value);
  }

  /**
   * The same as {@link
   * com.datastrato.graviton.rel.transforms.Transforms#identity(java.lang.String[])}, but is usually
   * used as second argument for expression partitioning {@link
   * com.datastrato.graviton.rel.transforms.Transforms#function(java.lang.String,
   * com.datastrato.graviton.rel.transforms.Transform[])}.
   *
   * @param fieldName The field name to reference.
   * @return The named reference.
   */
  public static NamedReference field(String[] fieldName) {
    return new Transforms.NamedReference(fieldName);
  }

  /**
   * The same as {@link Transforms#field(String[])} but accepts a single column, use column name as
   * field name.
   *
   * @param column The column to reference.
   * @return The named reference.
   */
  public static NamedReference field(Column column) {
    return field(new String[] {column.name()});
  }

  /**
   * Creates a partitioning by function expression.
   *
   * @param name The function name.
   * @param args The function arguments, can be {@link NamedReference}, {@link LiteralReference} or
   *     {@link FunctionTrans}.
   * @return The function partitioning.
   */
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
