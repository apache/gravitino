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
package org.apache.gravitino.rel.types;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

/** The helper class for {@link Type}. */
public class Types {

  /** The data type representing `NULL` values. */
  public static class NullType implements Type {
    private static final NullType INSTANCE = new NullType();

    /** @return The singleton instance of {@link NullType}. */
    public static NullType get() {
      return INSTANCE;
    }

    private NullType() {}

    @Override
    public Name name() {
      return Name.NULL;
    }

    @Override
    public String simpleString() {
      return "null";
    }
  }

  /** The boolean type in Gravitino. */
  public static class BooleanType extends Type.PrimitiveType {
    private static final BooleanType INSTANCE = new BooleanType();

    /** @return The singleton instance of {@link BooleanType}. */
    public static BooleanType get() {
      return INSTANCE;
    }

    private BooleanType() {}

    @Override
    public Name name() {
      return Name.BOOLEAN;
    }

    @Override
    public String simpleString() {
      return "boolean";
    }
  }

  /** The byte type in Gravitino. */
  public static class ByteType extends Type.IntegralType {
    private static final ByteType INSTANCE = new ByteType(true);
    private static final ByteType UNSIGNED_INSTANCE = new ByteType(false);

    /** @return The singleton instance of unsigned byte type */
    public static ByteType unsigned() {
      return UNSIGNED_INSTANCE;
    }

    /** @return The singleton instance of {@link ByteType}. */
    public static ByteType get() {
      return INSTANCE;
    }

    private ByteType(boolean signed) {
      super(signed);
    }

    @Override
    public Name name() {
      return Name.BYTE;
    }

    @Override
    public String simpleString() {
      return signed() ? "byte" : "byte unsigned";
    }
  }

  /** The short type in Gravitino. */
  public static class ShortType extends Type.IntegralType {
    private static final ShortType INSTANCE = new ShortType(true);
    private static final ShortType UNSIGNED_INSTANCE = new ShortType(false);

    /** @return The singleton instance of unsigned short type */
    public static ShortType unsigned() {
      return UNSIGNED_INSTANCE;
    }

    /** @return The singleton instance of {@link ShortType}. */
    public static ShortType get() {
      return INSTANCE;
    }

    private ShortType(boolean signed) {
      super(signed);
    }

    @Override
    public Name name() {
      return Name.SHORT;
    }

    @Override
    public String simpleString() {
      return signed() ? "short" : "short unsigned";
    }
  }

  /** The integer type in Gravitino. */
  public static class IntegerType extends Type.IntegralType {
    private static final IntegerType INSTANCE = new IntegerType(true);
    private static final IntegerType UNSIGNED_INSTANCE = new IntegerType(false);

    /** @return The singleton instance of unsigned integer type */
    public static IntegerType unsigned() {
      return UNSIGNED_INSTANCE;
    }

    /** @return The singleton instance of {@link IntegerType}. */
    public static IntegerType get() {
      return INSTANCE;
    }

    private IntegerType(boolean signed) {
      super(signed);
    }

    @Override
    public Name name() {
      return Name.INTEGER;
    }

    @Override
    public String simpleString() {
      return signed() ? "integer" : "integer unsigned";
    }
  }

  /** The long type in Gravitino. */
  public static class LongType extends Type.IntegralType {
    private static final LongType INSTANCE = new LongType(true);
    private static final LongType UNSIGNED_INSTANCE = new LongType(false);

    /** @return The singleton instance of unsigned long type */
    public static LongType unsigned() {
      return UNSIGNED_INSTANCE;
    }

    /** @return The singleton instance of {@link LongType}. */
    public static LongType get() {
      return INSTANCE;
    }

    private LongType(boolean signed) {
      super(signed);
    }

    @Override
    public Name name() {
      return Name.LONG;
    }

    @Override
    public String simpleString() {
      return signed() ? "long" : "long unsigned";
    }
  }

  /** The float type in Gravitino. */
  public static class FloatType extends Type.FractionType {
    private static final FloatType INSTANCE = new FloatType();

    /** @return The singleton instance of {@link FloatType}. */
    public static FloatType get() {
      return INSTANCE;
    }

    private FloatType() {}

    @Override
    public Name name() {
      return Name.FLOAT;
    }

    @Override
    public String simpleString() {
      return "float";
    }
  }

  /** The double type in Gravitino. */
  public static class DoubleType extends Type.FractionType {
    private static final DoubleType INSTANCE = new DoubleType();

    /** @return The singleton instance of {@link DoubleType}. */
    public static DoubleType get() {
      return INSTANCE;
    }

    private DoubleType() {}

    @Override
    public Name name() {
      return Name.DOUBLE;
    }

    @Override
    public String simpleString() {
      return "double";
    }
  }

  /** The decimal type in Gravitino. */
  public static class DecimalType extends Type.FractionType {
    /**
     * @param precision The precision of the decimal type.
     * @param scale The scale of the decimal type.
     * @return A {@link DecimalType} with the given precision and scale.
     */
    public static DecimalType of(int precision, int scale) {
      return new DecimalType(precision, scale);
    }

    private final int precision;
    private final int scale;

    private DecimalType(int precision, int scale) {
      checkPrecisionScale(precision, scale);
      this.precision = precision;
      this.scale = scale;
    }

    static void checkPrecisionScale(int precision, int scale) {
      Preconditions.checkArgument(
          precision > 0 && precision <= 38,
          "Decimal precision must be in range[1, 38]: precision: %s",
          precision);
      Preconditions.checkArgument(
          scale >= 0 && scale <= precision,
          "Decimal scale must be in range [0, precision (%s)]: scala: %s",
          precision,
          scale);
    }

    /** @return The name of the decimal type. */
    @Override
    public Name name() {
      return Name.DECIMAL;
    }

    /** @return The precision of the decimal type. */
    public int precision() {
      return precision;
    }

    /** @return The scale of the decimal type. */
    public int scale() {
      return scale;
    }

    @Override
    public String simpleString() {
      return String.format("decimal(%d,%d)", precision, scale);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DecimalType)) {
        return false;
      }
      DecimalType that = (DecimalType) o;
      return precision == that.precision && scale == that.scale;
    }

    @Override
    public int hashCode() {
      return Objects.hash(precision, scale);
    }
  }

  /** The date time type in Gravitino. */
  public static class DateType extends Type.DateTimeType {
    private static final DateType INSTANCE = new DateType();

    /** @return The singleton instance of {@link DateType}. */
    public static DateType get() {
      return INSTANCE;
    }

    private DateType() {}

    /** @return The name of the date type. */
    @Override
    public Name name() {
      return Name.DATE;
    }

    @Override
    public String simpleString() {
      return "date";
    }
  }

  /** The time type in Gravitino. */
  public static class TimeType extends Type.DateTimeType {
    private static final TimeType INSTANCE = new TimeType();

    /** @return The singleton instance of {@link TimeType}. */
    public static TimeType get() {
      return INSTANCE;
    }

    /**
     * @param precision The precision of the time type.
     * @return A {@link TimeType} with the given precision.
     */
    public static TimeType of(int precision) {
      Preconditions.checkArgument(
          precision >= MIN_ALLOWED_PRECISION && precision <= MAX_ALLOWED_PRECISION,
          "precision must be in range [%s, %s]: precision: %s",
          MIN_ALLOWED_PRECISION,
          MAX_ALLOWED_PRECISION,
          precision);
      return new TimeType(precision);
    }

    private final int precision;

    private TimeType() {
      this(DATE_TIME_PRECISION_NOT_SET);
    }

    private TimeType(int precision) {
      this.precision = precision;
    }

    /** @return The precision of the time type. */
    public int precision() {
      return precision;
    }

    /** @return True if the time type has precision set, false otherwise. */
    public boolean hasPrecisionSet() {
      return precision != DATE_TIME_PRECISION_NOT_SET;
    }

    @Override
    public Name name() {
      return Name.TIME;
    }

    @Override
    public String simpleString() {
      return hasPrecisionSet() ? String.format("time(%d)", precision) : "time";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TimeType)) return false;
      TimeType that = (TimeType) o;
      return precision == that.precision;
    }

    @Override
    public int hashCode() {
      return precision;
    }

    @Override
    public String toString() {
      return "TimeType{" + "precision=" + precision + '}';
    }
  }

  /** The timestamp type in Gravitino. */
  public static class TimestampType extends Type.DateTimeType {
    private static final TimestampType INSTANCE_WITHOUT_TIME_ZONE = new TimestampType(false);
    private static final TimestampType INSTANCE_WITH_TIME_ZONE = new TimestampType(true);

    /** @return A {@link TimestampType} without time zone. */
    public static TimestampType withoutTimeZone() {
      return INSTANCE_WITHOUT_TIME_ZONE;
    }

    /** @return A {@link TimestampType} with time zone. */
    public static TimestampType withTimeZone() {
      return INSTANCE_WITH_TIME_ZONE;
    }

    /**
     * @param precision The precision of the timestamp type.
     * @return A {@link TimestampType} with the given precision and without time zone.
     */
    public static TimestampType withoutTimeZone(int precision) {
      Preconditions.checkArgument(
          precision >= MIN_ALLOWED_PRECISION && precision <= MAX_ALLOWED_PRECISION,
          "precision must be in range [%s, %s]: precision: %s",
          MIN_ALLOWED_PRECISION,
          MAX_ALLOWED_PRECISION,
          precision);
      return new TimestampType(false, precision);
    }

    /**
     * @param precision The precision of the timestamp type.
     * @return A {@link TimestampType} with the given precision and time zone.
     */
    public static TimestampType withTimeZone(int precision) {
      Preconditions.checkArgument(
          precision >= MIN_ALLOWED_PRECISION && precision <= MAX_ALLOWED_PRECISION,
          "precision must be in range [%s, %s]: precision: %s",
          MIN_ALLOWED_PRECISION,
          MAX_ALLOWED_PRECISION,
          precision);
      return new TimestampType(true, precision);
    }

    private final boolean withTimeZone;
    private final int precision;

    private TimestampType(boolean withTimeZone) {
      this(withTimeZone, DATE_TIME_PRECISION_NOT_SET);
    }

    private TimestampType(boolean withTimeZone, int precision) {
      this.withTimeZone = withTimeZone;
      this.precision = precision;
    }

    /** @return True if the timestamp type has time zone, false otherwise. */
    public boolean hasTimeZone() {
      return withTimeZone;
    }

    /** @return The precision of the timestamp type. */
    public int precision() {
      return precision;
    }

    /** @return True if the timestamp type has precision set, false otherwise. */
    public boolean hasPrecisionSet() {
      return precision != DATE_TIME_PRECISION_NOT_SET;
    }

    @Override
    public Name name() {
      return Name.TIMESTAMP;
    }

    /** @return The simple string representation of the timestamp type. */
    @Override
    public String simpleString() {
      return hasPrecisionSet()
          ? withTimeZone
              ? String.format("timestamp_tz(%d)", precision)
              : String.format("timestamp(%d)", precision)
          : withTimeZone ? "timestamp_tz" : "timestamp";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TimestampType)) return false;
      TimestampType that = (TimestampType) o;
      return withTimeZone == that.withTimeZone && precision == that.precision;
    }

    @Override
    public int hashCode() {
      return Objects.hash(withTimeZone, precision);
    }

    @Override
    public String toString() {
      return "TimestampType{" + "withTimeZone=" + withTimeZone + ", precision=" + precision + '}';
    }
  }

  /** The interval year type in Gravitino. */
  public static class IntervalYearType extends Type.IntervalType {
    private static final IntervalYearType INSTANCE = new IntervalYearType();

    /** @return The singleton instance of {@link IntervalYearType}. */
    public static IntervalYearType get() {
      return INSTANCE;
    }

    private IntervalYearType() {}

    @Override
    public Name name() {
      return Name.INTERVAL_YEAR;
    }

    @Override
    public String simpleString() {
      return "interval_year";
    }
  }

  /** The interval day type in Gravitino. */
  public static class IntervalDayType extends Type.IntervalType {
    private static final IntervalDayType INSTANCE = new IntervalDayType();

    /** @return The singleton instance of {@link IntervalDayType}. */
    public static IntervalDayType get() {
      return INSTANCE;
    }

    private IntervalDayType() {}

    @Override
    public Name name() {
      return Name.INTERVAL_DAY;
    }

    @Override
    public String simpleString() {
      return "interval_day";
    }
  }

  /**
   * The string type in Gravitino, equivalent to varchar(MAX), which the MAX is determined by the
   * underlying catalog.
   */
  public static class StringType extends Type.PrimitiveType {
    private static final StringType INSTANCE = new StringType();

    /** @return The singleton instance of {@link StringType}. */
    public static StringType get() {
      return INSTANCE;
    }

    private StringType() {}

    @Override
    public Name name() {
      return Name.STRING;
    }

    @Override
    public String simpleString() {
      return "string";
    }
  }

  /** The uuid type in Gravitino. */
  public static class UUIDType extends Type.PrimitiveType {
    private static final UUIDType INSTANCE = new UUIDType();

    /** @return The singleton instance of {@link UUIDType}. */
    public static UUIDType get() {
      return INSTANCE;
    }

    private UUIDType() {}

    @Override
    public Name name() {
      return Name.UUID;
    }

    @Override
    public String simpleString() {
      return "uuid";
    }
  }

  /**
   * Fixed-length byte array type, if you want to use variable-length byte array, use {@link
   * BinaryType} instead.
   */
  public static class FixedType extends Type.PrimitiveType {

    /**
     * @param length The length of the fixed type.
     * @return An {@link FixedType} with the given length.
     */
    public static FixedType of(int length) {
      return new FixedType(length);
    }

    private final int length;

    private FixedType(int length) {
      this.length = length;
    }

    @Override
    public Name name() {
      return Name.FIXED;
    }

    /** @return The length of the fixed type. */
    public int length() {
      return length;
    }

    @Override
    public String simpleString() {
      return String.format("fixed(%d)", length);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FixedType)) {
        return false;
      }
      FixedType fixedType = (FixedType) o;
      return length == fixedType.length;
    }

    @Override
    public int hashCode() {
      return Objects.hash(length);
    }
  }

  /** The varchar type in Gravitino. */
  public static class VarCharType extends Type.PrimitiveType {

    /**
     * @param length The length of the var char type.
     * @return An {@link VarCharType} with the given length.
     */
    public static VarCharType of(int length) {
      return new VarCharType(length);
    }

    private final int length;

    private VarCharType(int length) {
      this.length = length;
    }

    @Override
    public Name name() {
      return Name.VARCHAR;
    }

    /** @return The length of the var char type. */
    public int length() {
      return length;
    }

    @Override
    public String simpleString() {
      return String.format("varchar(%d)", length);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof VarCharType)) {
        return false;
      }
      VarCharType that = (VarCharType) o;
      return length == that.length;
    }

    @Override
    public int hashCode() {
      return Objects.hash(length);
    }
  }

  /** The fixed char type in Gravitino. */
  public static class FixedCharType extends Type.PrimitiveType {

    /**
     * @param length The length of the fixed char type.
     * @return An {@link FixedCharType} with the given length.
     */
    public static FixedCharType of(int length) {
      return new FixedCharType(length);
    }

    private final int length;

    private FixedCharType(int length) {
      this.length = length;
    }

    @Override
    public Name name() {
      return Name.FIXEDCHAR;
    }

    /** @return The length of the fixed char type. */
    public int length() {
      return length;
    }

    @Override
    public String simpleString() {
      return String.format("char(%d)", length);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FixedCharType)) {
        return false;
      }
      FixedCharType that = (FixedCharType) o;
      return length == that.length;
    }

    @Override
    public int hashCode() {
      return Objects.hash(length);
    }
  }

  /** The binary type in Gravitino. */
  public static class BinaryType extends Type.PrimitiveType {
    private static final BinaryType INSTANCE = new BinaryType();

    /** @return The singleton instance of {@link BinaryType}. */
    public static BinaryType get() {
      return INSTANCE;
    }

    private BinaryType() {}

    @Override
    public Name name() {
      return Name.BINARY;
    }

    @Override
    public String simpleString() {
      return "binary";
    }
  }

  /** The struct type in Gravitino. */
  public static class StructType extends Type.ComplexType {

    /**
     * @param fields The fields of the struct type.
     * @return An {@link StructType} with the given fields.
     */
    public static StructType of(Field... fields) {
      Preconditions.checkArgument(
          fields != null && fields.length > 0, "fields cannot be null or empty");
      return new StructType(fields);
    }

    private final Field[] fields;

    private StructType(Field[] fields) {
      this.fields = fields;
    }

    /** @return The fields of the struct type. */
    public Field[] fields() {
      return fields;
    }

    @Override
    public Name name() {
      return Name.STRUCT;
    }

    @Override
    public String simpleString() {
      StringJoiner separator = new StringJoiner(",", "struct<", ">");
      Arrays.stream(fields).forEach(field -> separator.add(field.simpleString()));
      return separator.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StructType)) {
        return false;
      }
      StructType that = (StructType) o;
      return Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(fields);
    }

    /** A field of a struct type. */
    public static class Field {

      /**
       * @param name The name of the field.
       * @param type The type of the field.
       * @return A NOT NULL {@link Field} with the given name, type and empty comment.
       */
      public static Field notNullField(String name, Type type) {
        return notNullField(name, type, null);
      }

      /**
       * @param name The name of the field.
       * @param type The type of the field.
       * @param comment The comment of the field.
       * @return A NOT NULL {@link Field} with the given name, type and comment.
       */
      public static Field notNullField(String name, Type type, String comment) {
        return of(name, type, false, comment);
      }

      /**
       * @param name The name of the field.
       * @param type The type of the field.
       * @return A nullable {@link Field} with the given name, type and empty comment.
       */
      public static Field nullableField(String name, Type type) {
        return nullableField(name, type, null);
      }

      /**
       * @param name The name of the field.
       * @param type The type of the field.
       * @param comment The comment of the field.
       * @return A nullable {@link Field} with the given name, type and comment.
       */
      public static Field nullableField(String name, Type type, String comment) {
        return of(name, type, true, comment);
      }

      /**
       * @param name The name of the field.
       * @param type The type of the field.
       * @param nullable Whether the field is nullable.
       * @param comment The comment of the field.
       * @return A nullable {@link Field} with the given name, type and comment.
       */
      public static Field of(String name, Type type, boolean nullable, String comment) {
        return new Field(name, type, nullable, comment);
      }

      private final String name;
      private final Type type;
      private final boolean nullable;
      private final String comment;

      private Field(String name, Type type, boolean nullable, String comment) {
        Preconditions.checkArgument(name != null, "name cannot be null");
        Preconditions.checkArgument(type != null, "type cannot be null");
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.comment = comment;
      }

      /** @return The name of the field. */
      public String name() {
        return name;
      }

      /** @return The type of the field. */
      public Type type() {
        return type;
      }

      /** @return Whether the field is nullable. */
      public boolean nullable() {
        return nullable;
      }

      /** @return The comment of the field. May be null if not set. */
      public String comment() {
        return comment;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof Field)) {
          return false;
        }
        Field field = (Field) o;
        return nullable == field.nullable
            && Objects.equals(name, field.name)
            && Objects.equals(type, field.type);
      }

      @Override
      public int hashCode() {
        return Objects.hash(name, type, nullable);
      }

      /** @return The simple string representation of the field. */
      public String simpleString() {
        return String.format(
            "%s: %s %s COMMENT %s",
            name,
            type.simpleString(),
            nullable ? "NULL" : "NOT NULL",
            comment == null ? "" : "'" + comment + "'");
      }
    }
  }

  /** The list type in Gravitino. */
  public static class ListType extends Type.ComplexType {

    /**
     * Create a new {@link ListType} with the given element type and the type is nullable.
     *
     * @param elementType The element type of the list.
     * @return A new {@link ListType} instance.
     */
    public static ListType nullable(Type elementType) {
      return of(elementType, true);
    }

    /**
     * Create a new {@link ListType} with the given element type.
     *
     * @param elementType The element type of the list.
     * @return A new {@link ListType} instance.
     */
    public static ListType notNull(Type elementType) {
      return of(elementType, false);
    }

    /**
     * Create a new {@link ListType} with the given element type and whether the element is
     * nullable.
     *
     * @param elementType The element type of the list.
     * @param elementNullable Whether the element of the list is nullable.
     * @return A new {@link ListType} instance.
     */
    public static ListType of(Type elementType, boolean elementNullable) {
      return new ListType(elementType, elementNullable);
    }

    private final Type elementType;
    private final boolean elementNullable;

    private ListType(Type elementType, boolean elementNullable) {
      Preconditions.checkArgument(elementType != null, "elementType cannot be null");
      this.elementType = elementType;
      this.elementNullable = elementNullable;
    }

    /** @return The element type of the list. */
    public Type elementType() {
      return elementType;
    }

    /** @return Whether the element of the list is nullable. */
    public boolean elementNullable() {
      return elementNullable;
    }

    @Override
    public Name name() {
      return Name.LIST;
    }

    @Override
    public String simpleString() {
      return elementNullable
          ? "list<" + elementType.simpleString() + ">"
          : "list<" + elementType.simpleString() + ", NOT NULL>";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ListType)) {
        return false;
      }
      ListType listType = (ListType) o;
      return elementNullable == listType.elementNullable
          && Objects.equals(elementType, listType.elementType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(elementType, elementNullable);
    }
  }

  /** The map type in Gravitino. */
  public static class MapType extends Type.ComplexType {

    /**
     * Create a new {@link MapType} with the given key type, value type and the value is nullable.
     *
     * @param keyType The key type of the map.
     * @param valueType The value type of the map.
     * @return A new {@link MapType} instance.
     */
    public static MapType valueNullable(Type keyType, Type valueType) {
      return of(keyType, valueType, true);
    }

    /**
     * Create a new {@link MapType} with the given key type, value type and the value is not
     * nullable.
     *
     * @param keyType The key type of the map.
     * @param valueType The value type of the map.
     * @return A new {@link MapType} instance.
     */
    public static MapType valueNotNull(Type keyType, Type valueType) {
      return of(keyType, valueType, false);
    }

    /**
     * Create a new {@link MapType} with the given key type, value type and whether the value is
     * nullable
     *
     * @param keyType The key type of the map.
     * @param valueType The value type of the map.
     * @param valueNullable Whether the value of the map is nullable.
     * @return A new {@link MapType} instance.
     */
    public static MapType of(Type keyType, Type valueType, boolean valueNullable) {
      return new MapType(keyType, valueType, valueNullable);
    }

    private final Type keyType;
    private final Type valueType;
    private final boolean valueNullable;

    private MapType(Type keyType, Type valueType, boolean valueNullable) {
      this.keyType = keyType;
      this.valueType = valueType;
      this.valueNullable = valueNullable;
    }

    @Override
    public Name name() {
      return Name.MAP;
    }

    /** @return The key type of the map. */
    public Type keyType() {
      return keyType;
    }

    /** @return The value type of the map. */
    public Type valueType() {
      return valueType;
    }

    /** @return Whether the value of the map is nullable. */
    public boolean valueNullable() {
      return valueNullable;
    }

    @Override
    public String simpleString() {
      return valueNullable
          ? "map<" + keyType.simpleString() + "," + valueType.simpleString() + ">"
          : "map<" + keyType.simpleString() + "," + valueType.simpleString() + ", NOT NULL>";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof MapType)) {
        return false;
      }
      MapType mapType = (MapType) o;
      return valueNullable == mapType.valueNullable
          && Objects.equals(keyType, mapType.keyType)
          && Objects.equals(valueType, mapType.valueType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyType, valueType, valueNullable);
    }
  }

  /** The union type in Gravitino. */
  public static class UnionType extends Type.ComplexType {

    /**
     * Create a new {@link UnionType} with the given types.
     *
     * @param types The types of the union.
     * @return A new {@link UnionType} instance.
     */
    public static UnionType of(Type... types) {
      return new UnionType(types);
    }

    private final Type[] types;

    private UnionType(Type[] types) {
      this.types = types;
    }

    /** @return The types of the union. */
    public Type[] types() {
      return types;
    }

    @Override
    public Name name() {
      return Name.UNION;
    }

    @Override
    public String simpleString() {
      StringJoiner separator = new StringJoiner(",", "union<", ">");
      Arrays.stream(types).forEach(type -> separator.add(type.simpleString()));
      return separator.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof UnionType)) {
        return false;
      }
      UnionType unionType = (UnionType) o;
      return Arrays.equals(types, unionType.types);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(types);
    }
  }

  /**
   * Represents a type that is not parsed yet. The parsed type is represented by other types of
   * {@link Types}.
   */
  public static class UnparsedType implements Type {

    /**
     * Creates a new {@link UnparsedType} with the given unparsed type.
     *
     * @param unparsedType The unparsed type.
     * @return A new {@link UnparsedType} with the given unparsed type.
     */
    public static UnparsedType of(String unparsedType) {
      return new UnparsedType(unparsedType);
    }

    private final String unparsedType;

    private UnparsedType(String unparsedType) {
      this.unparsedType = unparsedType;
    }

    /** @return The unparsed type as a string. */
    public String unparsedType() {
      return unparsedType;
    }

    @Override
    public Name name() {
      return Name.UNPARSED;
    }

    @Override
    public String simpleString() {
      return String.format("unparsed(%s)", unparsedType);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof UnparsedType)) {
        return false;
      }
      UnparsedType that = (UnparsedType) o;
      return Objects.equals(unparsedType, that.unparsedType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(unparsedType);
    }

    @Override
    public String toString() {
      return unparsedType;
    }
  }

  /** Represents a type that is defined in an external catalog. */
  public static class ExternalType implements Type {
    private final String catalogString;

    /**
     * Creates a new {@link ExternalType} with the given catalog string.
     *
     * @param catalogString The string representation of this type in the catalog.
     * @return A new {@link ExternalType} with the given catalog string.
     */
    public static ExternalType of(String catalogString) {
      return new ExternalType(catalogString);
    }

    private ExternalType(String catalogString) {
      this.catalogString = catalogString;
    }

    /** @return The string representation of this type in external catalog. */
    public String catalogString() {
      return catalogString;
    }

    @Override
    public Name name() {
      return Name.EXTERNAL;
    }

    @Override
    public String simpleString() {
      return String.format("external(%s)", catalogString);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ExternalType)) {
        return false;
      }
      ExternalType that = (ExternalType) o;
      return Objects.equals(catalogString, that.catalogString);
    }

    @Override
    public int hashCode() {
      return Objects.hash(catalogString);
    }

    @Override
    public String toString() {
      return simpleString();
    }
  }

  /**
   * @param dataType The data type to check.
   * @return True if the given data type is allowed to be an auto-increment column.
   */
  public static boolean allowAutoIncrement(Type dataType) {
    return dataType instanceof IntegerType || dataType instanceof LongType;
  }

  private Types() {}
}
