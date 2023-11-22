/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.types;

/** An interface representing all data types supported by Gravitino. */
public interface Type {
  /** Returns the generic name of the type. */
  Name name();

  /** Readable string representation for the type. */
  String simpleString();

  enum Name {
    BOOLEAN,
    BYTE,
    SHORT,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATE,
    TIME,
    TIMESTAMP,
    INTERVAL_YEAR,
    INTERVAL_DAY,
    STRING,
    VARCHAR,
    FIXEDCHAR,
    UUID,
    FIXED,
    BINARY,
    STRUCT,
    LIST,
    MAP,
    UNION
  }

  /** The base type of all primitive types. */
  abstract class PrimitiveType implements Type {}

  /** The base type of all numeric types. */
  abstract class NumericType extends PrimitiveType {}

  /** The base type of all date/time types. */
  abstract class DateTimeType extends PrimitiveType {}

  /** The base type of all interval types. */
  abstract class IntervalType extends PrimitiveType {}

  /** The base type of all complex types, including struct, list, map, and union. */
  abstract class ComplexType implements Type {}

  /** The base type of all integral types. */
  abstract class Integral extends NumericType {}

  /** The base type of all fractional types. */
  abstract class FractionType extends NumericType {}
}
