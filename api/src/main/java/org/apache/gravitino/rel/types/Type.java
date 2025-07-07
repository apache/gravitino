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

import org.apache.gravitino.annotation.Evolving;

/** An interface representing all data types supported by Apache Gravitino. */
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
    UNPARSED,

    /** The external type. An external type represents a type that is not supported by Gravitino. */
    EXTERNAL
  }

  /** The base type of all primitive types. */
  abstract class PrimitiveType implements Type {}

  /** The base type of all numeric types. */
  abstract class NumericType extends PrimitiveType {}

  /** The base type of all date/time types. */
  abstract class DateTimeType extends PrimitiveType {
    /**
     * Indicates that precision for the date/time type was not explicitly set by the user. The value
     * should be converted to the catalog's default precision.
     */
    protected static final int DATE_TIME_PRECISION_NOT_SET = -1;
    /**
     * Represents the minimum precision range for timestamp, time and other date/time types. The
     * minimum precision is 0, which means second-level precision.
     */
    protected static final int MIN_ALLOWED_PRECISION = 0;
    /**
     * Represents the maximum precision allowed for timestamp, time and other date/time types. The
     * maximum precision is 12, which means picosecond-level precision.
     */
    protected static final int MAX_ALLOWED_PRECISION = 12;
  }

  /** The base type of all interval types. */
  abstract class IntervalType extends PrimitiveType {}

  /** The base type of all complex types, including struct, list, map, and union. */
  abstract class ComplexType implements Type {}

  /** The base type of all integral types. */
  abstract class IntegralType extends NumericType {
    private final boolean signed;

    /** @param signed or unsigned of the integer type. */
    public IntegralType(boolean signed) {
      this.signed = signed;
    }

    /** @return True if the integer type has signed, false otherwise. */
    public boolean signed() {
      return signed;
    }
  }

  /** The base type of all fractional types. */
  abstract class FractionType extends NumericType {}
}
