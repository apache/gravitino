/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.types;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

/** The helper class for {@link Type}. */
public class Types {

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
    private static final ByteType INSTANCE = new ByteType();

    /** @return The singleton instance of {@link ByteType}. */
    public static ByteType get() {
      return INSTANCE;
    }

    private ByteType() {}

    @Override
    public Name name() {
      return Name.BYTE;
    }

    @Override
    public String simpleString() {
      return "byte";
    }
  }

  /** The short type in Gravitino. */
  public static class ShortType extends Type.IntegralType {
    private static final ShortType INSTANCE = new ShortType();

    /** @return The singleton instance of {@link ShortType}. */
    public static ShortType get() {
      return INSTANCE;
    }

    private ShortType() {}

    @Override
    public Name name() {
      return Name.SHORT;
    }

    @Override
    public String simpleString() {
      return "short";
    }
  }

  /** The integer type in Gravitino. */
  public static class IntegerType extends Type.IntegralType {
    private static final IntegerType INSTANCE = new IntegerType();

    /** @return The singleton instance of {@link IntegerType}. */
    public static IntegerType get() {
      return INSTANCE;
    }

    private IntegerType() {}

    @Override
    public Name name() {
      return Name.INTEGER;
    }

    @Override
    public String simpleString() {
      return "integer";
    }
  }

  /** The long type in Gravitino. */
  public static class LongType extends Type.IntegralType {
    private static final LongType INSTANCE = new LongType();

    /** @return The singleton instance of {@link LongType}. */
    public static LongType get() {
      return INSTANCE;
    }

    private LongType() {}

    @Override
    public Name name() {
      return Name.LONG;
    }

    @Override
    public String simpleString() {
      return "long";
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
      Preconditions.checkArgument(
          precision <= 38,
          "Decimals with precision larger than 38 are not supported: %s",
          precision);
      Preconditions.checkArgument(
          scale <= precision, "Scale cannot be larger than precision: %s > %s", scale, precision);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public Name name() {
      return Name.DECIMAL;
    }

    public int precision() {
      return precision;
    }

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
      if (o == null || getClass() != o.getClass()) {
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

    private TimeType() {}

    @Override
    public Name name() {
      return Name.TIME;
    }

    @Override
    public String simpleString() {
      return "time";
    }
  }

  /** The timestamp type in Gravitino. */
  public static class TimestampType extends Type.DateTimeType {
    private static final TimestampType INSTANCE_WITHOUT_TIME_ZONE = new TimestampType(false);
    private static final TimestampType INSTANCE_WITH_TIME_ZONE = new TimestampType(true);

    /** @return A {@link TimestampType} with time zone. */
    public static TimestampType withTimeZone() {
      return INSTANCE_WITH_TIME_ZONE;
    }

    /** @return A {@link TimestampType} without time zone. */
    public static TimestampType withoutTimeZone() {
      return INSTANCE_WITHOUT_TIME_ZONE;
    }

    private final boolean withTimeZone;

    private TimestampType(boolean withTimeZone) {
      this.withTimeZone = withTimeZone;
    }

    public boolean hasTimeZone() {
      return withTimeZone;
    }

    @Override
    public Name name() {
      return Name.TIMESTAMP;
    }

    @Override
    public String simpleString() {
      return withTimeZone ? "timestamp_tz" : "timestamp";
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
      if (o == null || getClass() != o.getClass()) {
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
      if (o == null || getClass() != o.getClass()) {
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
      if (o == null || getClass() != o.getClass()) {
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

  /**
   * The struct type in Gravitino. Note, this type is not supported in the current version of
   * Gravitino.
   */
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
      if (o == null || getClass() != o.getClass()) {
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
        if (o == null || getClass() != o.getClass()) {
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

  /** A list type. Note, this type is not supported in the current version of Gravitino. */
  public static class ListType extends Type.ComplexType {

    public static ListType nullable(Type elementType) {
      return of(elementType, true);
    }

    public static ListType notNull(Type elementType) {
      return of(elementType, false);
    }

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

    public Type elementType() {
      return elementType;
    }

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
      if (o == null || getClass() != o.getClass()) {
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

  /**
   * The map type in Gravitino. Note, this type is not supported in the current version of
   * Gravitino.
   */
  public static class MapType extends Type.ComplexType {

    public static MapType valueNullable(Type keyType, Type valueType) {
      return of(keyType, valueType, true);
    }

    public static MapType valueNotNull(Type keyType, Type valueType) {
      return of(keyType, valueType, false);
    }

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

    public Type keyType() {
      return keyType;
    }

    public Type valueType() {
      return valueType;
    }

    public boolean valueNullable() {
      return valueNullable;
    }

    @Override
    public String simpleString() {
      return "map<" + keyType.simpleString() + "," + valueType.simpleString() + ">";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
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

  /**
   * The union type in Gravitino. Note, this type is not supported in the current version of
   * Gravitino.
   */
  public static class UnionType extends Type.ComplexType {

    public static UnionType of(Type... types) {
      return new UnionType(types);
    }

    private final Type[] types;

    private UnionType(Type[] types) {
      this.types = types;
    }

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
      if (o == null || getClass() != o.getClass()) {
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
   * @param dataType The data type to check.
   * @return True if the given data type is allowed to be an auto-increment column.
   */
  public static boolean allowAutoIncrement(Type dataType) {
    return dataType instanceof IntegerType || dataType instanceof LongType;
  }
}
