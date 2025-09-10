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

package org.apache.gravitino.cli;

import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/** Utility class for converting type names to Type instances. */
public class TypeConverter {

  /**
   * Converts a type name string to the appropriate Type.
   *
   * @param typeName The string representing the data type (e.g., "boolean", "byte", "string").
   * @return An instance of the appropriate Type.
   * @throws IllegalArgumentException if the type name is not recognized.
   */
  public static Type convert(String typeName) {
    switch (typeName.toLowerCase()) {
      case "null":
        return Types.NullType.get();
      case "boolean":
        return Types.BooleanType.get();
      case "byte":
        return Types.ByteType.get();
      case "ubyte":
        return Types.ByteType.unsigned();
      case "short":
        return Types.ShortType.get();
      case "ushort":
        return Types.ShortType.unsigned();
      case "integer":
        return Types.IntegerType.get();
      case "uinteger":
        return Types.IntegerType.unsigned();
      case "long":
        return Types.LongType.get();
      case "ulong":
        return Types.LongType.unsigned();
      case "float":
        return Types.FloatType.get();
      case "double":
        return Types.DoubleType.get();
      case "date":
        return Types.DateType.get();
      case "time":
        return Types.TimeType.get();
      case "timestamp":
        return Types.TimestampType.withoutTimeZone();
      case "tztimestamp":
        return Types.TimestampType.withTimeZone();
      case "intervalyear":
        return Types.IntervalYearType.get();
      case "intervalday":
        return Types.IntervalDayType.get();
      case "uuid":
        return Types.UUIDType.get();
      case "string":
        return Types.StringType.get();
      case "binary":
        return Types.BinaryType.get();
      default:
        throw new IllegalArgumentException("Unknown or unsupported type: " + typeName);
    }
  }

  /**
   * Converts a type name string to the appropriate Type.
   *
   * @param typeName The string representing the data type (e.g., "fixed" or "varchar").
   * @param length Length of the data type.
   * @return An instance of the appropriate Type.
   * @throws IllegalArgumentException if the type name is not recognized.
   */
  public static Type convert(String typeName, int length) {
    if (typeName.toLowerCase().startsWith("fixed")) {
      return Types.FixedType.of(length);
    } else if (typeName.toLowerCase().startsWith("varchar")) {
      return Types.VarCharType.of(length);
    } else if (typeName.toLowerCase().startsWith("char")) {
      return Types.FixedCharType.of(length);
    } else {
      throw new IllegalArgumentException(
          "Unknown or unsupported variable length type: " + typeName);
    }
  }

  /**
   * Converts a type name string to the appropriate Type.
   *
   * @param typeName The string representing the data type. Only "decimal" is supported.
   * @param precision Precision of the decimal.
   * @param scale Scale of the decimal.
   * @return An instance of the appropriate Type.
   * @throws IllegalArgumentException if the type name is not recognized.
   */
  public static Type convert(String typeName, int precision, int scale) {
    if (typeName.toLowerCase().startsWith("decimal")) {
      return Types.DecimalType.of(precision, scale);
    } else {
      throw new IllegalArgumentException(
          "Unknown or unsupported precision and scale type: " + typeName);
    }
  }
}
