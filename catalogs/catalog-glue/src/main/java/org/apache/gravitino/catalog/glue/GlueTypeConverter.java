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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.connector.DataTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/**
 * Converts between AWS Glue / Hive type strings and Gravitino {@link Type} objects.
 *
 * <p>Glue stores column types as Hive type strings (e.g. {@code "bigint"}, {@code "decimal(10,2)"},
 * {@code "array<string>"}). This converter handles all primitive and complex types natively;
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
  static final String ARRAY = "array";
  static final String MAP = "map";
  static final String STRUCT = "struct";
  static final String UNIONTYPE = "uniontype";

  // Matches name(body): char(10), varchar(255), decimal(10,2)
  private static final Pattern PAREN_TYPE_PATTERN = Pattern.compile("(\\w+)\\(([^)]*)\\)");
  // Matches name<body>: array<string>, map<string,int>, struct<id:bigint>
  // Greedy (.+) correctly handles nesting: array<map<string,int>> → group(2)=map<string,int>
  private static final Pattern ANGLE_TYPE_PATTERN = Pattern.compile("(\\w+)<(.+)>");

  @Override
  public Type toGravitino(String glueType) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(glueType), "Glue column type must not be blank");
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

    // name(body) types: char(N), varchar(N), decimal(P,S)
    Matcher m = PAREN_TYPE_PATTERN.matcher(lower);
    if (m.matches()) {
      String typeName = m.group(1);
      String body = m.group(2).trim();
      try {
        switch (typeName) {
          case CHAR:
            return Types.FixedCharType.of(Integer.parseInt(body));
          case VARCHAR:
            return Types.VarCharType.of(Integer.parseInt(body));
          case DECIMAL:
            String[] ps = body.split(",", 2);
            int precision = Integer.parseInt(ps[0].trim());
            int scale = ps.length > 1 ? Integer.parseInt(ps[1].trim()) : 0;
            return Types.DecimalType.of(precision, scale);
          default:
            return Types.ExternalType.of(glueType);
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid Glue type: " + glueType, e);
      }
    }

    // name<body> types: array<T>, map<K,V>, struct<...>, uniontype<...>
    m = ANGLE_TYPE_PATTERN.matcher(lower);
    if (m.matches()) {
      String typeName = m.group(1);
      String body = m.group(2).trim();
      switch (typeName) {
        case ARRAY:
          return Types.ListType.nullable(toGravitino(body));
        case MAP:
          {
            List<String> parts = splitTopLevel(body);
            if (parts.size() == 2) {
              return Types.MapType.valueNullable(
                  toGravitino(parts.get(0)), toGravitino(parts.get(1)));
            }
            throw new IllegalArgumentException("Invalid Glue type: " + glueType);
          }
        case STRUCT:
          {
            List<String> tokens = splitTopLevel(body);
            Types.StructType.Field[] fields = new Types.StructType.Field[tokens.size()];
            for (int i = 0; i < tokens.size(); i++) {
              String token = tokens.get(i);
              int colonIdx = token.indexOf(':');
              if (colonIdx < 0) {
                throw new IllegalArgumentException("Invalid Glue type: " + glueType);
              }
              fields[i] =
                  Types.StructType.Field.nullableField(
                      token.substring(0, colonIdx).trim(),
                      toGravitino(token.substring(colonIdx + 1).trim()));
            }
            return Types.StructType.of(fields);
          }
        case UNIONTYPE:
          {
            List<String> parts = splitTopLevel(body);
            Type[] types = parts.stream().map(this::toGravitino).toArray(Type[]::new);
            return Types.UnionType.of(types);
          }
        default:
          return Types.ExternalType.of(glueType);
      }
    }

    // Unknown types are preserved as ExternalType so the original string survives the round-trip.
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
      return String.format("%s(%d)", CHAR, ((Types.FixedCharType) type).length());
    }
    if (type instanceof Types.VarCharType) {
      return String.format("%s(%d)", VARCHAR, ((Types.VarCharType) type).length());
    }
    if (type instanceof Types.DecimalType) {
      Types.DecimalType d = (Types.DecimalType) type;
      return String.format("%s(%d,%d)", DECIMAL, d.precision(), d.scale());
    }
    if (type instanceof Types.ListType) {
      return String.format("%s<%s>", ARRAY, fromGravitino(((Types.ListType) type).elementType()));
    }
    if (type instanceof Types.MapType) {
      Types.MapType mapType = (Types.MapType) type;
      return String.format(
          "%s<%s,%s>", MAP, fromGravitino(mapType.keyType()), fromGravitino(mapType.valueType()));
    }
    if (type instanceof Types.StructType) {
      String fields =
          Arrays.stream(((Types.StructType) type).fields())
              .map(f -> String.format("%s:%s", f.name(), fromGravitino(f.type())))
              .collect(Collectors.joining(","));
      return String.format("%s<%s>", STRUCT, fields);
    }
    if (type instanceof Types.UnionType) {
      String types =
          Arrays.stream(((Types.UnionType) type).types())
              .map(this::fromGravitino)
              .collect(Collectors.joining(","));
      return String.format("%s<%s>", UNIONTYPE, types);
    }
    if (type instanceof Types.ExternalType) {
      return ((Types.ExternalType) type).catalogString();
    }
    throw new IllegalArgumentException("Unsupported Gravitino type for Glue: " + type);
  }

  /**
   * Splits {@code s} on commas that are not nested inside {@code <...>} angle brackets.
   *
   * <p>For example, {@code "string,map<string,int>"} splits into {@code ["string",
   * "map<string,int>"]}.
   */
  private static List<String> splitTopLevel(String s) {
    List<String> parts = new ArrayList<>();
    int depth = 0;
    int start = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '<') {
        depth++;
      } else if (c == '>') {
        depth--;
      } else if (c == ',' && depth == 0) {
        parts.add(s.substring(start, i).trim());
        start = i + 1;
      }
    }
    parts.add(s.substring(start).trim());
    return parts;
  }
}
