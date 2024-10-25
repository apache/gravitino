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
package org.apache.gravitino.rel.expressions.transforms;

import com.google.common.collect.ObjectArrays;
import java.util.Arrays;
import java.util.Objects;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.RangePartition;

/** Helper methods to create logical transforms to pass into Apache Gravitino. */
public class Transforms {
  /** An empty array of transforms. */
  public static final Transform[] EMPTY_TRANSFORM = new Transform[0];
  /** The name of the identity transform. */
  public static final String NAME_OF_IDENTITY = "identity";
  /** The name of the year transform. The year transform returns the year of the input value. */
  public static final String NAME_OF_YEAR = "year";

  /** The name of the month transform. The month transform returns the month of the input value. */
  public static final String NAME_OF_MONTH = "month";

  /** The name of the day transform. The day transform returns the day of the input value. */
  public static final String NAME_OF_DAY = "day";

  /** The name of the hour transform. The hour transform returns the hour of the input value. */
  public static final String NAME_OF_HOUR = "hour";

  /**
   * The name of the bucket transform. The bucket transform returns the bucket of the input value.
   */
  public static final String NAME_OF_BUCKET = "bucket";

  /**
   * The name of the truncate transform. The truncate transform returns the truncated value of the
   */
  public static final String NAME_OF_TRUNCATE = "truncate";

  /** The name of the list transform. The list transform includes multiple fields in a list. */
  public static final String NAME_OF_LIST = "list";

  /** The name of the range transform. The range transform returns the range of the input value. */
  public static final String NAME_OF_RANGE = "range";

  /**
   * Create a transform that returns the input value.
   *
   * @param fieldName The field name to transform
   * @return The created transform
   */
  public static IdentityTransform identity(String[] fieldName) {
    return identity(NamedReference.field(fieldName));
  }

  /**
   * Create a transform that returns the input value.
   *
   * @param columnName The column name to transform
   * @return The created transform
   */
  public static IdentityTransform identity(String columnName) {
    return identity(new String[] {columnName});
  }

  /**
   * Create a transform that returns the year of the input value.
   *
   * @param fieldName The field name to transform
   * @return The created transform
   */
  public static YearTransform year(String[] fieldName) {
    return year(NamedReference.field(fieldName));
  }

  /**
   * Create a transform that returns the year of the input value.
   *
   * @param columnName The column name to transform
   * @return The created transform
   */
  public static YearTransform year(String columnName) {
    return year(new String[] {columnName});
  }

  /**
   * Create a transform that returns the month of the input value.
   *
   * @param fieldName The field name to transform
   * @return The created transform
   */
  public static MonthTransform month(String[] fieldName) {
    return month(NamedReference.field(fieldName));
  }

  /**
   * Create a transform that returns the month of the input value.
   *
   * @param columnName The column name to transform
   * @return The created transform
   */
  public static MonthTransform month(String columnName) {
    return month(new String[] {columnName});
  }

  /**
   * Create a transform that returns the day of the input value.
   *
   * @param fieldName The field name to transform
   * @return The created transform
   */
  public static DayTransform day(String[] fieldName) {
    return day(NamedReference.field(fieldName));
  }

  /**
   * Create a transform that returns the day of the input value.
   *
   * @param columnName The column name to transform
   * @return The created transform
   */
  public static DayTransform day(String columnName) {
    return day(new String[] {columnName});
  }

  /**
   * Create a transform that returns the hour of the input value.
   *
   * @param fieldName The field name to transform
   * @return The created transform
   */
  public static HourTransform hour(String[] fieldName) {
    return hour(NamedReference.field(fieldName));
  }

  /**
   * Create a transform that returns the hour of the input value.
   *
   * @param columnName The column name to transform
   * @return The created transform
   */
  public static HourTransform hour(String columnName) {
    return hour(new String[] {columnName});
  }

  /**
   * Create a transform that returns the bucket of the input value.
   *
   * @param numBuckets The number of buckets to use
   * @param fieldNames The field names to transform
   * @return The created transform
   */
  public static BucketTransform bucket(int numBuckets, String[]... fieldNames) {
    return new BucketTransform(
        Literals.integerLiteral(numBuckets),
        Arrays.stream(fieldNames).map(NamedReference::field).toArray(NamedReference[]::new));
  }

  /**
   * Create a transform that includes multiple fields in a list.
   *
   * @param fieldNames The field names to include in the list
   * @return The created transform
   */
  public static ListTransform list(String[]... fieldNames) {
    return list(fieldNames, new ListPartition[0]);
  }

  /**
   * Create a transform that includes multiple fields in a list with preassigned list partitions.
   *
   * @param fieldNames The field names to include in the list
   * @param assignments The preassigned list partitions
   * @return The created transform
   */
  public static ListTransform list(String[][] fieldNames, ListPartition[] assignments) {
    return new ListTransform(
        Arrays.stream(fieldNames).map(NamedReference::field).toArray(NamedReference[]::new),
        assignments);
  }

  /**
   * Create a transform that returns the range of the input value.
   *
   * @param fieldName The field name to transform
   * @return The created transform
   */
  public static RangeTransform range(String[] fieldName) {
    return range(fieldName, new RangePartition[0]);
  }

  /**
   * Create a transform that returns the range of the input value with preassigned range partitions.
   *
   * @param fieldName The field name to transform
   * @param assignments The preassigned range partitions
   * @return The created transform
   */
  public static RangeTransform range(String[] fieldName, RangePartition[] assignments) {
    return new RangeTransform(NamedReference.field(fieldName), assignments);
  }

  /**
   * Create a transform that returns the truncated value of the input value with the given width.
   *
   * @param width The width to truncate to
   * @param fieldName The field name to transform
   * @return The created transform
   */
  public static TruncateTransform truncate(int width, String[] fieldName) {
    return new TruncateTransform(Literals.integerLiteral(width), NamedReference.field(fieldName));
  }

  /**
   * Create a transform that returns the truncated value of the input value with the given width.
   *
   * @param width The width to truncate to
   * @param columnName The column name to transform
   * @return The created transform
   */
  public static TruncateTransform truncate(int width, String columnName) {
    return truncate(width, new String[] {columnName});
  }

  /**
   * Create a transform that applies a function to the input value.
   *
   * @param name The name of the function to apply
   * @param arguments The arguments to the function
   * @return The created transform
   */
  public static ApplyTransform apply(String name, Expression[] arguments) {
    return new ApplyTransform(name, arguments);
  }

  /**
   * Create an identity transform that applies a function to the input value.
   *
   * @param ref The reference to transform.
   * @return An identity transform.
   */
  private static IdentityTransform identity(NamedReference ref) {
    return new IdentityTransform(ref);
  }

  /**
   * Create a year transform that applies a function to the input value.
   *
   * @param ref The reference to transform.
   * @return A year transform.
   */
  private static YearTransform year(NamedReference ref) {
    return new YearTransform(ref);
  }

  /**
   * Create a month transform that applies a function to the input value.
   *
   * @param ref The reference to transform.
   * @return A month transform.
   */
  private static MonthTransform month(NamedReference ref) {
    return new MonthTransform(ref);
  }

  /**
   * Create a day transform that applies a function to the input value.
   *
   * @param ref The reference to transform.
   * @return A day transform.
   */
  private static DayTransform day(NamedReference ref) {
    return new DayTransform(ref);
  }

  /**
   * Create an hour transform that applies a function to the input value.
   *
   * @param ref The reference to transform.
   * @return An hour transform.
   */
  private static HourTransform hour(NamedReference ref) {
    return new HourTransform(ref);
  }

  /** A transform that returns the input value. */
  public static final class IdentityTransform extends Transform.SingleFieldTransform {
    private IdentityTransform(NamedReference ref) {
      this.ref = ref;
    }

    /** @return The name of the transform. */
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

  /** A transform that returns the year of the input value. */
  public static final class YearTransform extends Transform.SingleFieldTransform {
    private YearTransform(NamedReference ref) {
      this.ref = ref;
    }

    /** @return The name of the transform. */
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

  /** A transform that returns the month of the input value. */
  public static final class MonthTransform extends Transform.SingleFieldTransform {
    private MonthTransform(NamedReference ref) {
      this.ref = ref;
    }

    /** @return The name of the transform. */
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

  /** A transform that returns the day of the input value. */
  public static final class DayTransform extends Transform.SingleFieldTransform {
    private DayTransform(NamedReference ref) {
      this.ref = ref;
    }

    /** @return The name of the transform. */
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

  /** A transform that returns the hour of the input value. */
  public static final class HourTransform extends Transform.SingleFieldTransform {
    private HourTransform(NamedReference ref) {
      this.ref = ref;
    }

    /** @return The name of the transform. */
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

  /** A transform that returns the bucket of the input value. */
  public static final class BucketTransform implements Transform {

    private final Literal<Integer> numBuckets;
    private final NamedReference[] fields;

    private BucketTransform(Literal<Integer> numBuckets, NamedReference[] fields) {
      this.numBuckets = numBuckets;
      this.fields = fields;
    }

    /** @return The number of buckets to use. */
    public int numBuckets() {
      return numBuckets.value();
    }

    /** @return The field names to transform. */
    public String[][] fieldNames() {
      return Arrays.stream(fields).map(NamedReference::fieldName).toArray(String[][]::new);
    }

    /** @return The name of the transform. */
    @Override
    public String name() {
      return NAME_OF_BUCKET;
    }

    /** @return The arguments to the transform. */
    @Override
    public Expression[] arguments() {
      return ObjectArrays.concat(new Expression[] {numBuckets}, fields, Expression.class);
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

  /** A transform that returns the truncated value of the input value with the given width. */
  public static final class TruncateTransform implements Transform {

    private final Literal<Integer> width;
    private final NamedReference field;

    private TruncateTransform(Literal<Integer> width, NamedReference field) {
      this.width = width;
      this.field = field;
    }

    /** @return The width to truncate to. */
    public int width() {
      return width.value();
    }

    /** @return The field name to transform. */
    public String[] fieldName() {
      return field.fieldName();
    }

    /** @return The name of the transform. */
    @Override
    public String name() {
      return NAME_OF_TRUNCATE;
    }

    /** @return The arguments to the transform. */
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

  /** A transform that includes multiple fields in a list. */
  public static final class ListTransform implements Transform {

    private final NamedReference[] fields;
    private final ListPartition[] assignments;

    private ListTransform(NamedReference[] fields) {
      this.fields = fields;
      this.assignments = new ListPartition[0];
    }

    private ListTransform(NamedReference[] fields, ListPartition[] assignments) {
      this.fields = fields;
      this.assignments = assignments;
    }

    /** @return The field names to include in the list. */
    public String[][] fieldNames() {
      return Arrays.stream(fields).map(NamedReference::fieldName).toArray(String[][]::new);
    }

    /** @return The name of the transform. */
    @Override
    public String name() {
      return NAME_OF_LIST;
    }

    /** @return The arguments to the transform. */
    @Override
    public Expression[] arguments() {
      return fields;
    }

    /** @return The assignments to the transform. */
    @Override
    public ListPartition[] assignments() {
      return assignments;
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

  /** A transform that returns the range of the input value. */
  public static final class RangeTransform implements Transform {

    private final NamedReference field;
    private final RangePartition[] assignments;

    private RangeTransform(NamedReference field) {
      this.field = field;
      this.assignments = new RangePartition[0];
    }

    private RangeTransform(NamedReference field, RangePartition[] assignments) {
      this.field = field;
      this.assignments = assignments;
    }

    /** @return The field name to transform. */
    public String[] fieldName() {
      return field.fieldName();
    }

    /** @return The name of the transform. */
    @Override
    public String name() {
      return NAME_OF_RANGE;
    }

    /** @return The arguments to the transform. */
    @Override
    public Expression[] arguments() {
      return new Expression[] {field};
    }

    /** @return The assignments to the transform. */
    @Override
    public RangePartition[] assignments() {
      return assignments;
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

  /** A transform that applies a function to the input value. */
  public static final class ApplyTransform implements Transform {

    private final String name;
    private final Expression[] arguments;

    private ApplyTransform(String name, Expression[] arguments) {
      this.name = name;
      this.arguments = arguments;
    }

    /** @return The name of the function to apply. */
    @Override
    public String name() {
      return name;
    }

    /** @return The arguments to the function. */
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

  private Transforms() {}
}
