/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.types;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

/** The helper class for {@link Type}. */
public class Types {

  public static class BooleanType extends Type.PrimitiveType {
    private static final BooleanType INSTANCE = new BooleanType();

    /** Returns the singleton instance of {@link BooleanType}. */
    public static BooleanType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.BOOLEAN;
    }

    @Override
    public String simpleString() {
      return "boolean";
    }
  }

  public static class ByteType extends Type.Integral {
    private static final ByteType INSTANCE = new ByteType();

    /** Returns the singleton instance of {@link ByteType}. */
    public static ByteType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.BYTE;
    }

    @Override
    public String simpleString() {
      return "byte";
    }
  }

  public static class ShortType extends Type.Integral {
    private static final ShortType INSTANCE = new ShortType();

    /** Returns the singleton instance of {@link ShortType}. */
    public static ShortType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.SHORT;
    }

    @Override
    public String simpleString() {
      return "short";
    }
  }

  public static class IntegerType extends Type.Integral {
    private static final IntegerType INSTANCE = new IntegerType();

    /** Returns the singleton instance of {@link IntegerType}. */
    public static IntegerType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.INTEGER;
    }

    @Override
    public String simpleString() {
      return "integer";
    }
  }

  public static class LongType extends Type.Integral {
    private static final LongType INSTANCE = new LongType();

    /** Returns the singleton instance of {@link LongType}. */
    public static LongType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.LONG;
    }

    @Override
    public String simpleString() {
      return "long";
    }
  }

  public static class FloatType extends Type.FractionType {
    private static final FloatType INSTANCE = new FloatType();

    /** Returns the singleton instance of {@link FloatType}. */
    public static FloatType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.FLOAT;
    }

    @Override
    public String simpleString() {
      return "float";
    }
  }

  public static class DoubleType extends Type.FractionType {
    private static final DoubleType INSTANCE = new DoubleType();

    /** Returns the singleton instance of {@link DoubleType}. */
    public static DoubleType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.DOUBLE;
    }

    @Override
    public String simpleString() {
      return "double";
    }
  }

  public static class DecimalType extends Type.FractionType {
    /** Returns a {@link DecimalType} with the given precision and scale. */
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

  public static class DateType extends Type.DateTimeType {
    private static final DateType INSTANCE = new DateType();

    /** Returns the singleton instance of {@link DateType}. */
    public static DateType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.DATE;
    }

    @Override
    public String simpleString() {
      return "date";
    }
  }

  public static class TimeType extends Type.DateTimeType {
    private static final TimeType INSTANCE = new TimeType();

    /** Returns the singleton instance of {@link TimeType}. */
    public static TimeType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.TIME;
    }

    @Override
    public String simpleString() {
      return "time";
    }
  }

  public static class TimestampType extends Type.DateTimeType {
    private static final TimestampType INSTANCE_WITHOUT_TIME_ZONE = new TimestampType(false);
    private static final TimestampType INSTANCE_WITH_TIME_ZONE = new TimestampType(true);

    /** Returns a {@link TimestampType} with time zone. */
    public static TimestampType withTimeZone() {
      return INSTANCE_WITH_TIME_ZONE;
    }

    /** Returns a {@link TimestampType} without time zone. */
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

  public static class IntervalYearType extends Type.IntervalType {
    private static final IntervalYearType INSTANCE = new IntervalYearType();

    /** Returns the singleton instance of {@link IntervalYearType}. */
    public static IntervalYearType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.INTERVAL_YEAR;
    }

    @Override
    public String simpleString() {
      return "interval_year";
    }
  }

  public static class IntervalDayType extends Type.IntervalType {
    private static final IntervalDayType INSTANCE = new IntervalDayType();

    /** Returns the singleton instance of {@link IntervalDayType}. */
    public static IntervalDayType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.INTERVAL_DAY;
    }

    @Override
    public String simpleString() {
      return "interval_day";
    }
  }

  public static class StringType extends Type.PrimitiveType {
    private static final StringType INSTANCE = new StringType();

    /** Returns the singleton instance of {@link StringType}. */
    public static StringType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.STRING;
    }

    @Override
    public String simpleString() {
      return "string";
    }
  }

  public static class UUIDType extends Type.PrimitiveType {
    private static final UUIDType INSTANCE = new UUIDType();

    /** Returns the singleton instance of {@link UUIDType}. */
    public static UUIDType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.UUID;
    }

    @Override
    public String simpleString() {
      return "uuid";
    }
  }

  public static class FixedType extends Type.PrimitiveType {

    /** Returns a {@link FixedType} with the given length. */
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

  public static class VarCharType extends Type.PrimitiveType {

    /** Returns a {@link VarCharType} with the given length. */
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

  public static class FixedCharType extends Type.PrimitiveType {

    /** Returns a {@link FixedCharType} with the given length. */
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

  public static class BinaryType extends Type.PrimitiveType {
    private static final BinaryType INSTANCE = new BinaryType();

    /** Returns the singleton instance of {@link BinaryType}. */
    public static BinaryType get() {
      return INSTANCE;
    }

    @Override
    public Name name() {
      return Name.BINARY;
    }

    @Override
    public String simpleString() {
      return "binary";
    }
  }

  public static class StructType extends Type.ComplexType {

    /** Returns a {@link StructType} with the given fields. */
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

    public static class Field {

      /** Returns a NOT NULL {@link Field} with the given name, type and empty comment. */
      public static Field notNullField(String name, Type type) {
        return notNullField(name, type, null);
      }

      /** Returns a NOT NULL {@link Field} with the given name, type and comment. */
      public static Field notNullField(String name, Type type, String comment) {
        return of(name, type, false, comment);
      }

      /** Returns a nullable {@link Field} with the given name, type and empty comment. */
      public static Field nullableField(String name, Type type) {
        return nullableField(name, type, null);
      }

      /** Returns a nullable {@link Field} with the given name, type and comment. */
      public static Field nullableField(String name, Type type, String comment) {
        return of(name, type, true, comment);
      }

      /** Returns a nullable {@link Field} with the given name, type and empty comment. */
      public static Field of(String name, Type type) {
        return of(name, type, true, null);
      }

      /** Returns a nullable {@link Field} with the given name, type and comment. */
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

      /** Returns the name of the field. */
      public String name() {
        return name;
      }

      /** Returns the type of the field. */
      public Type type() {
        return type;
      }

      /** Returns whether the field is nullable. */
      public boolean nullable() {
        return nullable;
      }

      /** Returns the comment of the field. May be null if not set. */
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
            && Objects.equals(type, field.type)
            && Objects.equals(comment, field.comment);
      }

      @Override
      public int hashCode() {
        return Objects.hash(name, type, nullable, comment);
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
}
