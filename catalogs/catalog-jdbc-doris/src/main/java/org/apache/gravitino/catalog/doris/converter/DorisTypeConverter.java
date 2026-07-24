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
package org.apache.gravitino.catalog.doris.converter;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/** Type converter for Apache Doris. */
public class DorisTypeConverter extends JdbcTypeConverter {
  private static final int MAX_DATETIME_PRECISION = 6;

  static final String BOOLEAN = "boolean";
  static final String TINYINT = "tinyint";
  static final String SMALLINT = "smallint";
  static final String INT = "int";
  static final String BIGINT = "bigint";
  static final String FLOAT = "float";
  static final String DOUBLE = "double";
  static final String DECIMAL = "decimal";
  static final String DATETIME = "datetime";
  static final String CHAR = "char";
  static final String STRING = "string";
  static final String BINARY = "binary";
  static final String VARBINARY = "varbinary";
  static final String JSON = "json";
  static final String VARIANT = "variant";
  static final String IPV4 = "ipv4";
  static final String IPV6 = "ipv6";
  static final String LARGEINT = "largeint";
  static final String BITMAP = "bitmap";
  static final String HLL = "hll";
  static final String DATEV2 = "datev2";
  static final String BIGINT_UNSIGNED = "bigint unsigned";

  @Override
  public Type toGravitino(JdbcTypeBean typeBean) {
    String typeName = typeBean.getTypeName().toLowerCase();

    // Extract base type name by stripping parenthesized parameters.
    // SHOW CREATE TABLE returns full type strings like "int(11)", "decimal(10,2)",
    // but the switch matches base names like "int", "decimal".
    String baseType = typeName;
    int parenIndex = typeName.indexOf('(');
    if (parenIndex > 0) {
      baseType = typeName.substring(0, parenIndex);
    }

    // Handle datetime(N) format — parse precision from type string when not in typeBean
    if ("datetime".equals(baseType)) {
      if (typeBean.getDatetimePrecision() != null) {
        return Types.TimestampType.withoutTimeZone(typeBean.getDatetimePrecision());
      }
      if (parenIndex > 0 && typeName.endsWith(")")) {
        try {
          String precisionStr = typeName.substring(parenIndex + 1, typeName.length() - 1);
          int precision = Integer.parseInt(precisionStr);
          return Types.TimestampType.withoutTimeZone(precision);
        } catch (NumberFormatException e) {
          // Fall through to default datetime handling
        }
      }
      return Types.TimestampType.withoutTimeZone();
    }

    switch (baseType) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case TINYINT:
        return Types.ByteType.get();
      case SMALLINT:
        return Types.ShortType.get();
      case INT:
        return Types.IntegerType.get();
      case BIGINT:
        return Types.LongType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case DECIMAL:
        return parseTypeParamsOrExternal(
            typeName, parenIndex, typeBean.getColumnSize(), typeBean.getScale());
      case DATE:
      case DATEV2:
        return Types.DateType.get();
      case CHAR:
        return parseTypeParamsOrExternal(typeName, parenIndex, typeBean.getColumnSize(), null);
      case VARCHAR:
        return parseTypeParamsOrExternal(typeName, parenIndex, typeBean.getColumnSize(), null);
      case STRING:
      case TEXT:
        return Types.StringType.get();
      case BINARY:
      case VARBINARY:
        return Types.BinaryType.get();
        // Explicitly enumerate known Doris-specific types to signal intentional coverage,
        // even though the behaviour is the same as default. BIGINT_UNSIGNED is reachable
        // from SHOW CREATE TABLE parsing; JDBC getColumns() returns "BIGINT" for this type.
      case BIGINT_UNSIGNED:
      case JSON:
      case VARIANT:
      case IPV4:
      case IPV6:
      case LARGEINT:
      case BITMAP:
      case HLL:
        return Types.ExternalType.of(typeName);
      default:
        return Types.ExternalType.of(typeName);
    }
  }

  /**
   * Parse type parameters from the type string or typeBean values. For DECIMAL, returns
   * DecimalType(p1, p2). For CHAR/VARCHAR, returns FixedCharType/VarCharType with default fallback.
   * Returns ExternalType if the type string is malformed and cannot be parsed.
   */
  private static Type parseTypeParamsOrExternal(
      String typeName, int parenIndex, Integer beanParam1, Integer beanParam2) {
    int p1 = beanParam1 != null ? beanParam1 : 0;
    int p2 = beanParam2 != null ? beanParam2 : 0;
    if (p1 == 0 && parenIndex > 0 && typeName.endsWith(")")) {
      try {
        String[] parts = typeName.substring(parenIndex + 1, typeName.length() - 1).split(",");
        p1 = Integer.parseInt(parts[0].trim());
        p2 = parts.length >= 2 ? Integer.parseInt(parts[1].trim()) : 0;
      } catch (NumberFormatException e) {
        return Types.ExternalType.of(typeName);
      }
    }

    String baseType = parenIndex > 0 ? typeName.substring(0, parenIndex) : typeName;
    switch (baseType) {
      case DECIMAL:
        return Types.DecimalType.of(p1, p2);
      case CHAR:
        // 1 = minimum valid length for CHAR; fallback when JDBC metadata and type string
        // both lack length info (unlikely in practice)
        return Types.FixedCharType.of(p1 > 0 ? p1 : 1);
      case VARCHAR:
        // 255 = MySQL/Doris legacy default for VARCHAR; fallback when JDBC metadata and
        // type string both lack length info (unlikely in practice)
        return Types.VarCharType.of(p1 > 0 ? p1 : 255);
      default:
        return Types.ExternalType.of(typeName);
    }
  }

  @Override
  public String fromGravitino(Type type) {
    if (type instanceof Types.BooleanType) {
      return BOOLEAN;
    } else if (type instanceof Types.ByteType) {
      return TINYINT;
    } else if (type instanceof Types.ShortType) {
      return SMALLINT;
    } else if (type instanceof Types.IntegerType) {
      return INT;
    } else if (type instanceof Types.LongType) {
      return BIGINT;
    } else if (type instanceof Types.FloatType) {
      return FLOAT;
    } else if (type instanceof Types.DoubleType) {
      return DOUBLE;
    } else if (type instanceof Types.DecimalType) {
      return DECIMAL
          + "("
          + ((Types.DecimalType) type).precision()
          + ","
          + ((Types.DecimalType) type).scale()
          + ")";
    } else if (type instanceof Types.DateType) {
      return DATEV2;
    } else if (type instanceof Types.TimestampType) {
      Types.TimestampType timestampType = (Types.TimestampType) type;
      if (timestampType.hasTimeZone()) {
        throw unsupportedType(type, "Doris DATETIME does not store time-zone information");
      }
      if (timestampType.hasPrecisionSet() && timestampType.precision() > MAX_DATETIME_PRECISION) {
        throw unsupportedType(type, "fractional-second precision must be between 0 and 6");
      }
      return timestampType.hasPrecisionSet()
          ? String.format("%s(%d)", DATETIME, timestampType.precision())
          : DATETIME;
    } else if (type instanceof Types.VarCharType) {
      int length = ((Types.VarCharType) type).length();
      if (length < 1 || length > 65533) {
        throw new IllegalArgumentException(
            String.format(
                "Type %s is invalid, length should be between 1 and 65533", type.simpleString()));
      }
      return VARCHAR + "(" + ((Types.VarCharType) type).length() + ")";
    } else if (type instanceof Types.FixedCharType) {
      int length = ((Types.FixedCharType) type).length();
      if (length < 1 || length > 255) {
        throw new IllegalArgumentException(
            String.format(
                "Type %s is invalid, length should be between 1 and 255", type.simpleString()));
      }

      return CHAR + "(" + ((Types.FixedCharType) type).length() + ")";
    } else if (type instanceof Types.StringType) {
      return STRING;
    } else if (type instanceof Types.BinaryType) {
      return BINARY;
    } else if (type instanceof Types.VariantType) {
      throw unsupportedType(
          type,
          "MySQL JDBC metadata reports Doris VARIANT as UNKNOWN, so the catalog cannot preserve "
              + "the type on round-trip");
    } else if (type instanceof Types.NullType) {
      throw unsupportedType(type, "the null-only placeholder has no Doris column type");
    } else if (type instanceof Types.GeometryType) {
      throw unsupportedType(
          type,
          "Doris GEO uses String/Varchar storage and cannot preserve Geometry CRS metadata as a "
              + "column type");
    } else if (type instanceof Types.GeographyType) {
      throw unsupportedType(
          type,
          "Doris GEO has no Geography column type that preserves CRS and edge-algorithm metadata");
    } else if (type instanceof Types.ExternalType) {
      return ((Types.ExternalType) type).catalogString();
    }
    throw new IllegalArgumentException(
        String.format("Couldn't convert Gravitino type %s to Doris type", type.simpleString()));
  }

  private static IllegalArgumentException unsupportedType(Type type, String reason) {
    return new IllegalArgumentException(
        String.format("Doris does not support Gravitino type %s: %s", type.simpleString(), reason));
  }
}
