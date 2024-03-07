/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.types;

import com.datastrato.gravitino.annotation.Evolving;

/** An interface representing all data types supported by Gravitino. */
@Evolving
public interface Type {
  /** @return The generic name of the type. */
  Name name();

  /** @return A readable string representation for the type. */
  String simpleString();

  /** The root type name of this type. */
  enum Name {
    /** The boolean type. */
    BOOLEAN,
    /** The byte type. */
    BYTE,
    /** The short type. */
    SHORT,
    /** The integer type. */
    INTEGER,
    /** The long type. */
    LONG,
    /** The float type. */
    FLOAT,
    /** The double type. */
    DOUBLE,
    /** The decimal type. */
    DECIMAL,
    /** The date type. */
    DATE,
    /** The time type. */
    TIME,
    /** The timestamp type. */
    TIMESTAMP,
    /** The interval year type. */
    INTERVAL_YEAR,
    /** The interval month type. */
    INTERVAL_DAY,
    /** The interval day type. */
    STRING,
    /** The varchar type. */
    VARCHAR,
    /** The char type with fixed length */
    FIXEDCHAR,
    /** The UUID type. */
    UUID,
    /** The binary type with fixed length */
    FIXED,
    /** The binary type with variable length. The length is specified in the type itself. */
    BINARY,
    /**
     * The struct type. A struct type is a complex type that contains a set of named fields, each
     * with a type, and optionally a comment.
     */
    STRUCT,

    /**
     * The list type. A list type is a complex type that contains a set of elements, each with the
     * same type.
     */
    LIST,

    /**
     * The map type. A map type is a complex type that contains a set of key-value pairs, each with
     * a key type and a value type.
     */
    MAP,
    /** The union type. A union type is a complex type that contains a set of types. */
    UNION,

    /** The null type. A null type represents a value that is null. */
    NULL,

    /** The unparsed type. An unparsed type represents an unresolvable type. */
    UNPARSED
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
  abstract class IntegralType extends NumericType {}

  /** The base type of all fractional types. */
  abstract class FractionType extends NumericType {}
}
