/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.transforms;

import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.Literal;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.google.common.collect.ObjectArrays;
import java.util.Arrays;
import java.util.Objects;

/** Helper methods to create logical transforms to pass into Gravitino. */
public class Transforms {
  public static final Transform[] EMPTY_TRANSFORM = new Transform[0];
  public static final String NAME_OF_IDENTITY = "identity";
  public static final String NAME_OF_YEAR = "year";
  public static final String NAME_OF_MONTH = "month";
  public static final String NAME_OF_DAY = "day";
  public static final String NAME_OF_HOUR = "hour";
  public static final String NAME_OF_BUCKET = "bucket";
  public static final String NAME_OF_TRUNCATE = "truncate";
  public static final String NAME_OF_LIST = "list";
  public static final String NAME_OF_RANGE = "range";

  public static IdentityTransform identity(String... fieldName) {
    return identity(NamedReference.field(fieldName));
  }

  private static IdentityTransform identity(NamedReference ref) {
    return new IdentityTransform(ref);
  }

  public static YearTransform year(String... fieldName) {
    return year(NamedReference.field(fieldName));
  }

  private static YearTransform year(NamedReference ref) {
    return new YearTransform(ref);
  }

  public static MonthTransform month(String... fieldName) {
    return month(NamedReference.field(fieldName));
  }

  private static MonthTransform month(NamedReference ref) {
    return new MonthTransform(ref);
  }

  public static DayTransform day(String... fieldName) {
    return day(NamedReference.field(fieldName));
  }

  private static DayTransform day(NamedReference ref) {
    return new DayTransform(ref);
  }

  public static HourTransform hour(String... fieldName) {
    return hour(NamedReference.field(fieldName));
  }

  private static HourTransform hour(NamedReference ref) {
    return new HourTransform(ref);
  }

  public static BucketTransform bucket(int numBuckets, String[]... fieldNames) {
    return new BucketTransform(
        Literal.ofInteger(numBuckets),
        Arrays.stream(fieldNames).map(NamedReference::field).toArray(NamedReference[]::new));
  }

  public static ListTransform list(String[][] fieldNames) {
    return new ListTransform(
        Arrays.stream(fieldNames).map(NamedReference::field).toArray(NamedReference[]::new));
  }

  public static RangeTransform range(String[] fieldName) {
    return new RangeTransform(NamedReference.field(fieldName));
  }

  public static TruncateTransform truncate(int width, String... fieldName) {
    return new TruncateTransform(Literal.ofInteger(width), NamedReference.field(fieldName));
  }

  public static ApplyTransform apply(String name, Expression[] arguments) {
    return new ApplyTransform(name, arguments);
  }

  public static final class IdentityTransform extends Transform.SingleFieldTransform {
    private IdentityTransform(NamedReference ref) {
      this.ref = ref;
    }

    @Override
    public String name() {
      return NAME_OF_IDENTITY;
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  public static final class YearTransform extends Transform.SingleFieldTransform {
    private YearTransform(NamedReference ref) {
      this.ref = ref;
    }

    @Override
    public String name() {
      return NAME_OF_YEAR;
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  public static final class MonthTransform extends Transform.SingleFieldTransform {
    private MonthTransform(NamedReference ref) {
      this.ref = ref;
    }

    @Override
    public String name() {
      return NAME_OF_MONTH;
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  public static final class DayTransform extends Transform.SingleFieldTransform {
    private DayTransform(NamedReference ref) {
      this.ref = ref;
    }

    @Override
    public String name() {
      return NAME_OF_DAY;
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  public static final class HourTransform extends Transform.SingleFieldTransform {
    private HourTransform(NamedReference ref) {
      this.ref = ref;
    }

    @Override
    public String name() {
      return NAME_OF_HOUR;
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  public static final class BucketTransform implements Transform {

    private final Literal<Integer> numBuckets;
    private final NamedReference[] fields;

    private BucketTransform(Literal<Integer> numBuckets, NamedReference[] fields) {
      this.numBuckets = numBuckets;
      this.fields = fields;
    }

    public int numBuckets() {
      return numBuckets.value();
    }

    public String[][] fieldNames() {
      return Arrays.stream(fields).map(NamedReference::fieldName).toArray(String[][]::new);
    }

    @Override
    public String name() {
      return NAME_OF_BUCKET;
    }

    @Override
    public Expression[] arguments() {
      return ObjectArrays.concat(numBuckets, fields);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BucketTransform that = (BucketTransform) o;
      return Objects.equals(numBuckets, that.numBuckets) && Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(numBuckets);
      result = 31 * result + Arrays.hashCode(fields);
      return result;
    }
  }

  public static final class TruncateTransform implements Transform {

    private final Literal<Integer> width;
    private final NamedReference field;

    private TruncateTransform(Literal<Integer> width, NamedReference field) {
      this.width = width;
      this.field = field;
    }

    public int width() {
      return width.value();
    }

    public String[] fieldName() {
      return field.fieldName();
    }

    @Override
    public String name() {
      return NAME_OF_TRUNCATE;
    }

    @Override
    public Expression[] arguments() {
      return new Expression[] {width, field};
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TruncateTransform that = (TruncateTransform) o;
      return Objects.equals(width, that.width) && Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
      return Objects.hash(width, field);
    }
  }

  public static final class ListTransform implements Transform {

    private final NamedReference[] fields;

    private ListTransform(NamedReference[] fields) {
      this.fields = fields;
    }

    public String[][] fieldNames() {
      return Arrays.stream(fields).map(NamedReference::fieldName).toArray(String[][]::new);
    }

    @Override
    public String name() {
      return NAME_OF_LIST;
    }

    @Override
    public Expression[] arguments() {
      return fields;
    }

    @Override
    public Expression[] assignments() {
      // todo: resolve this
      return Transform.super.assignments();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ListTransform that = (ListTransform) o;
      return Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(fields);
    }
  }

  public static final class RangeTransform implements Transform {

    private final NamedReference field;

    private RangeTransform(NamedReference field) {
      this.field = field;
    }

    public String[] fieldName() {
      return field.fieldName();
    }

    @Override
    public String name() {
      return NAME_OF_RANGE;
    }

    @Override
    public Expression[] arguments() {
      return new Expression[] {field};
    }

    @Override
    public Expression[] assignments() {
      // todo: resolve this
      return Transform.super.assignments();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RangeTransform that = (RangeTransform) o;
      return Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field);
    }
  }

  public static final class ApplyTransform implements Transform {

    private final String name;
    private final Expression[] arguments;

    private ApplyTransform(String name, Expression[] arguments) {
      this.name = name;
      this.arguments = arguments;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Expression[] arguments() {
      return arguments;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ApplyTransform that = (ApplyTransform) o;
      return Objects.equals(name, that.name) && Arrays.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(name);
      result = 31 * result + Arrays.hashCode(arguments);
      return result;
    }
  }
}
