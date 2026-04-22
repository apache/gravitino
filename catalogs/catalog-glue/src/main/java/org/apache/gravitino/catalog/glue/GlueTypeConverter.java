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

  public static final GlueTypeConverter CONVERTER = new GlueTypeConverter();

  @Override
  public Type toGravitino(String glueType) {
    if (glueType == null || glueType.isEmpty()) {
      return Types.ExternalType.of("");
    }
    String lower = glueType.trim().toLowerCase(ROOT);

    switch (lower) {
      case "boolean":
        return Types.BooleanType.get();
      case "tinyint":
        return Types.ByteType.get();
      case "smallint":
        return Types.ShortType.get();
      case "int":
      case "integer":
        return Types.IntegerType.get();
      case "bigint":
        return Types.LongType.get();
      case "float":
        return Types.FloatType.get();
      case "double":
        return Types.DoubleType.get();
      case "string":
        return Types.StringType.get();
      case "date":
        return Types.DateType.get();
      case "timestamp":
        return Types.TimestampType.withoutTimeZone();
      case "binary":
        return Types.BinaryType.get();
      case "interval_year_month":
        return Types.IntervalYearType.get();
      case "interval_day_time":
        return Types.IntervalDayType.get();
      default:
        break;
    }

    // char(N)
    if (lower.startsWith("char(") && lower.endsWith(")")) {
      try {
        int length = Integer.parseInt(lower.substring(5, lower.length() - 1).trim());
        return Types.FixedCharType.of(length);
      } catch (NumberFormatException e) {
        return Types.ExternalType.of(glueType);
      }
    }
    // varchar(N)
    if (lower.startsWith("varchar(") && lower.endsWith(")")) {
      try {
        int length = Integer.parseInt(lower.substring(8, lower.length() - 1).trim());
        return Types.VarCharType.of(length);
      } catch (NumberFormatException e) {
        return Types.ExternalType.of(glueType);
      }
    }
    // decimal(P,S) or decimal(P, S)
    if (lower.startsWith("decimal(") && lower.endsWith(")")) {
      try {
        String inner = lower.substring(8, lower.length() - 1);
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
    if (type instanceof Types.BooleanType) return "boolean";
    if (type instanceof Types.ByteType) return "tinyint";
    if (type instanceof Types.ShortType) return "smallint";
    if (type instanceof Types.IntegerType) return "int";
    if (type instanceof Types.LongType) return "bigint";
    if (type instanceof Types.FloatType) return "float";
    if (type instanceof Types.DoubleType) return "double";
    if (type instanceof Types.StringType) return "string";
    if (type instanceof Types.DateType) return "date";
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
      return "timestamp";
    }
    if (type instanceof Types.BinaryType) return "binary";
    if (type instanceof Types.IntervalYearType) return "interval_year_month";
    if (type instanceof Types.IntervalDayType) return "interval_day_time";
    if (type instanceof Types.FixedCharType) {
      return "char(" + ((Types.FixedCharType) type).length() + ")";
    }
    if (type instanceof Types.VarCharType) {
      return "varchar(" + ((Types.VarCharType) type).length() + ")";
    }
    if (type instanceof Types.DecimalType) {
      Types.DecimalType d = (Types.DecimalType) type;
      return "decimal(" + d.precision() + "," + d.scale() + ")";
    }
    if (type instanceof Types.ExternalType) {
      return ((Types.ExternalType) type).catalogString();
    }
    throw new IllegalArgumentException("Unsupported Gravitino type for Glue: " + type);
  }
}
