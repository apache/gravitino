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
package org.apache.gravitino.catalog.glue;

import static java.util.Locale.ROOT;

import org.apache.gravitino.connector.DataTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/**
 * Converts between AWS Glue / Hive type strings and Gravitino {@link Type} objects.
 *
 * <p>Glue stores column types as Hive type strings (e.g. {@code "bigint"}, {@code "decimal(10,2)"},
 * {@code "array<string>"}). This converter handles all primitive types natively; complex and
 * unknown types fall back to {@link Types.ExternalType} to preserve the original string.
 */
public class GlueTypeConverter implements DataTypeConverter<String, String> {

  static final String BOOLEAN = "boolean";
  static final String TINYINT = "tinyint";
  static final String SMALLINT = "smallint";
  static final String INT = "int";
  static final String INTEGER = "integer";
  static final String BIGINT = "bigint";
  static final String FLOAT = "float";
  static final String DOUBLE = "double";
  static final String STRING = "string";
  static final String DATE = "date";
  static final String TIMESTAMP = "timestamp";
  static final String BINARY = "binary";
  static final String INTERVAL_YEAR_MONTH = "interval_year_month";
  static final String INTERVAL_DAY_TIME = "interval_day_time";
  static final String CHAR = "char";
  static final String VARCHAR = "varchar";
  static final String DECIMAL = "decimal";

  @Override
  public Type toGravitino(String glueType) {
    if (glueType == null || glueType.isEmpty()) {
      return Types.ExternalType.of("");
    }
    String lower = glueType.trim().toLowerCase(ROOT);

    switch (lower) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case TINYINT:
        return Types.ByteType.get();
      case SMALLINT:
        return Types.ShortType.get();
      case INT:
      case INTEGER:
        return Types.IntegerType.get();
      case BIGINT:
        return Types.LongType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case STRING:
        return Types.StringType.get();
      case DATE:
        return Types.DateType.get();
      case TIMESTAMP:
        return Types.TimestampType.withoutTimeZone();
      case BINARY:
        return Types.BinaryType.get();
      case INTERVAL_YEAR_MONTH:
        return Types.IntervalYearType.get();
      case INTERVAL_DAY_TIME:
        return Types.IntervalDayType.get();
      default:
        break;
    }

    // char(N)
    if (lower.startsWith(CHAR + "(") && lower.endsWith(")")) {
      try {
        int length = Integer.parseInt(lower.substring(CHAR.length() + 1, lower.length() - 1).trim());
        return Types.FixedCharType.of(length);
      } catch (NumberFormatException e) {
        return Types.ExternalType.of(glueType);
      }
    }
    // varchar(N)
    if (lower.startsWith(VARCHAR + "(") && lower.endsWith(")")) {
      try {
        int length =
            Integer.parseInt(lower.substring(VARCHAR.length() + 1, lower.length() - 1).trim());
        return Types.VarCharType.of(length);
      } catch (NumberFormatException e) {
        return Types.ExternalType.of(glueType);
      }
    }
    // decimal(P,S) or decimal(P, S)
    if (lower.startsWith(DECIMAL + "(") && lower.endsWith(")")) {
      try {
        String inner = lower.substring(DECIMAL.length() + 1, lower.length() - 1);
        String[] parts = inner.split(",", 2);
        int precision = Integer.parseInt(parts[0].trim());
        int scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
        return Types.DecimalType.of(precision, scale);
      } catch (NumberFormatException e) {
        return Types.ExternalType.of(glueType);
      }
    }

    // Complex types (array<...>, map<...>, struct<...>, uniontype<...>) and anything unknown
    // are preserved as ExternalType so the original string survives the round-trip.
    return Types.ExternalType.of(glueType);
  }

  @Override
  public String fromGravitino(Type type) {
    if (type instanceof Types.BooleanType) return BOOLEAN;
    if (type instanceof Types.ByteType) return TINYINT;
    if (type instanceof Types.ShortType) return SMALLINT;
    if (type instanceof Types.IntegerType) return INT;
    if (type instanceof Types.LongType) return BIGINT;
    if (type instanceof Types.FloatType) return FLOAT;
    if (type instanceof Types.DoubleType) return DOUBLE;
    if (type instanceof Types.StringType) return STRING;
    if (type instanceof Types.DateType) return DATE;
    if (type instanceof Types.TimestampType) {
      // Glue/Hive timestamps are timezoneless; see:
      // https://cwiki.apache.org/confluence/display/hive/languagemanual+types
      Types.TimestampType tsType = (Types.TimestampType) type;
      if (tsType.hasTimeZone()) {
        throw new IllegalArgumentException(
            "Unsupported Gravitino type for Glue: "
                + type
                + ". Glue/Hive does not support TIMESTAMP WITH TIME ZONE.");
      }
      return TIMESTAMP;
    }
    if (type instanceof Types.BinaryType) return BINARY;
    if (type instanceof Types.IntervalYearType) return INTERVAL_YEAR_MONTH;
    if (type instanceof Types.IntervalDayType) return INTERVAL_DAY_TIME;
    if (type instanceof Types.FixedCharType) {
      return CHAR + "(" + ((Types.FixedCharType) type).length() + ")";
    }
    if (type instanceof Types.VarCharType) {
      return VARCHAR + "(" + ((Types.VarCharType) type).length() + ")";
    }
    if (type instanceof Types.DecimalType) {
      Types.DecimalType d = (Types.DecimalType) type;
      return DECIMAL + "(" + d.precision() + "," + d.scale() + ")";
    }
    if (type instanceof Types.ExternalType) {
      return ((Types.ExternalType) type).catalogString();
    }
    throw new IllegalArgumentException("Unsupported Gravitino type for Glue: " + type);
  }
}
