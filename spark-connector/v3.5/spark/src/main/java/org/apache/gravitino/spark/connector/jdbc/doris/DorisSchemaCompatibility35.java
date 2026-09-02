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
package org.apache.gravitino.spark.connector.jdbc.doris;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;

/** Validates Gravitino logical schema against the physical Doris schema snapshot. */
final class DorisSchemaCompatibility35 {

  private static final int MAX_CATALYST_DECIMAL_PRECISION = 38;

  private DorisSchemaCompatibility35() {}

  static DorisReadSchema35 plan(
      Identifier identifier,
      Table logicalTable,
      DorisPhysicalSchema35 physicalSchema,
      SparkTypeConverter typeConverter) {
    Column[] logicalColumns = logicalTable.columns();
    StructField[] physicalFields = physicalSchema.schema().fields();
    if (logicalColumns.length != physicalFields.length) {
      throw incompatible(identifier, "logical and physical column counts differ");
    }

    List<StructField> visibleFields = new ArrayList<>(logicalColumns.length);
    List<String> projections = new ArrayList<>(logicalColumns.length);
    Map<String, String> normalizedTypeNames = new LinkedHashMap<>();
    boolean requiresSql = false;
    for (int index = 0; index < logicalColumns.length; index++) {
      Column logicalColumn = logicalColumns[index];
      StructField physicalField = physicalFields[index];
      String typeName = physicalSchema.dorisTypeName(index);
      validateIdentity(identifier, logicalColumn, physicalField, physicalSchema, index);
      if (!physicalSchema.catalystTypeResolved(index) && !isSupportedNormalizedType(typeName)) {
        throw incompatible(
            identifier,
            String.format(
                Locale.ROOT,
                "column %s has an unsupported Doris physical type: %s",
                logicalColumn.name(),
                typeName));
      }
      MetadataBuilder metadata = new MetadataBuilder().withMetadata(physicalField.metadata());
      if (logicalColumn.comment() != null) {
        metadata.putString("comment", logicalColumn.comment());
      }

      if (requiresNormalization(logicalColumn.dataType(), typeName)) {
        validateNormalizedType(
            identifier,
            logicalColumn.dataType(),
            physicalField,
            physicalSchema.catalystTypeResolved(index),
            typeName,
            typeConverter);
        visibleFields.add(
            DataTypes.createStructField(
                physicalField.name(),
                DataTypes.StringType,
                visibleNullable(logicalColumn, physicalSchema, index),
                metadata.build()));
        projections.add(normalizationProjection(physicalField.name(), typeName));
        normalizedTypeNames.put(physicalField.name(), typeName);
        requiresSql = true;
      } else {
        validateDirectTypeSignature(identifier, logicalColumn.dataType(), typeName);
        if (!typeConverter.toSparkType(logicalColumn.dataType()).equals(physicalField.dataType())) {
          throw incompatible(
              identifier,
              String.format(
                  Locale.ROOT,
                  "column %s type differs: logical=%s, physical=%s",
                  logicalColumn.name(),
                  logicalColumn.dataType().simpleString(),
                  physicalField.dataType().catalogString()));
        }
        visibleFields.add(
            DataTypes.createStructField(
                physicalField.name(),
                physicalField.dataType(),
                visibleNullable(logicalColumn, physicalSchema, index),
                metadata.build()));
        projections.add(DorisReadSchema35.quoteIdentifier(physicalField.name()));
      }
    }
    return new DorisReadSchema35(
        DataTypes.createStructType(visibleFields), projections, requiresSql, normalizedTypeNames);
  }

  private static void validateIdentity(
      Identifier identifier,
      Column logicalColumn,
      StructField physicalField,
      DorisPhysicalSchema35 physicalSchema,
      int index) {
    if (!logicalColumn.name().equalsIgnoreCase(physicalField.name())) {
      throw incompatible(
          identifier,
          String.format(
              Locale.ROOT,
              "column %d name differs: logical=%s, physical=%s",
              index,
              logicalColumn.name(),
              physicalField.name()));
    }
    if (physicalSchema.nullabilityKnown(index)
        && logicalColumn.nullable()
        && !physicalField.nullable()) {
      throw incompatible(
          identifier, "column " + logicalColumn.name() + " is less nullable in Doris");
    }
  }

  private static boolean visibleNullable(
      Column logicalColumn, DorisPhysicalSchema35 physicalSchema, int index) {
    return physicalSchema.nullabilityKnown(index)
        ? physicalSchema.schema().fields()[index].nullable()
        : logicalColumn.nullable();
  }

  private static boolean requiresNormalization(Type logicalType, String rawTypeName) {
    String typeName = baseType(rawTypeName);
    if (logicalType instanceof Types.ExternalType) {
      return true;
    }
    if (typeName.isEmpty()) {
      return logicalType instanceof Types.BinaryType;
    }
    if (isAlwaysNormalizedType(typeName)) {
      return true;
    }
    return false;
  }

  private static void validateNormalizedType(
      Identifier identifier,
      Type logicalType,
      StructField physicalField,
      boolean catalystTypeResolved,
      String rawTypeName,
      SparkTypeConverter typeConverter) {
    String typeName = baseType(rawTypeName);
    if (logicalType instanceof Types.ExternalType) {
      String logicalTypeName =
          canonicalTypeName(((Types.ExternalType) logicalType).catalogString());
      if (logicalTypeName.equals(canonicalTypeName(rawTypeName))) {
        return;
      }
    }
    if (logicalType instanceof Types.TimestampType) {
      Types.TimestampType timestampType = (Types.TimestampType) logicalType;
      int logicalPrecision = timestampType.hasPrecisionSet() ? timestampType.precision() : 0;
      if (!timestampType.hasTimeZone()
          && (typeName.equals("datetime") || typeName.equals("datetimev2"))
          && logicalPrecision == datetimePrecision(rawTypeName)) {
        return;
      }
    }
    if (logicalType instanceof Types.BinaryType
        && (typeName.equals("binary") || typeName.equals("varbinary"))) {
      return;
    }
    if ((logicalType instanceof Types.ListType
            || logicalType instanceof Types.MapType
            || logicalType instanceof Types.StructType)
        && catalystTypeResolved
        && typeConverter.toSparkType(logicalType).equals(physicalField.dataType())) {
      if ((logicalType instanceof Types.ListType && typeName.equals("array"))
          || (logicalType instanceof Types.MapType && typeName.equals("map"))
          || (logicalType instanceof Types.StructType && typeName.equals("struct"))) {
        return;
      }
    }
    if (logicalType instanceof Type.IntegralType
        && !((Type.IntegralType) logicalType).signed()
        && expectedUnsignedType(logicalType).equals(typeName)) {
      return;
    }
    if (logicalType instanceof Types.IntegerType
        && ((Types.IntegerType) logicalType).signed()
        && typeName.equals("largeint")) {
      // Doris 3.x exposes LARGEINT as INTEGER through MySQL-protocol JDBC metadata. The exact
      // physical COLUMN_TYPE remains authoritative for selecting lossless String normalization.
      return;
    }
    throw incompatible(
        identifier,
        String.format(
            Locale.ROOT,
            "logical type %s is incompatible with Doris type %s",
            logicalType.simpleString(),
            rawTypeName));
  }

  private static boolean isSupportedNormalizedType(String rawTypeName) {
    String typeName = baseType(rawTypeName);
    return isAlwaysNormalizedType(typeName) || isSafeDecimalFallback(rawTypeName);
  }

  private static void validateDirectTypeSignature(
      Identifier identifier, Type logicalType, String rawTypeName) {
    String typeName = baseType(rawTypeName);
    if (logicalType instanceof Types.VarCharType) {
      if (!typeName.equals("varchar")
          || ((Types.VarCharType) logicalType).length() != singleTypeParameter(rawTypeName)) {
        throw incompatible(identifier, "VARCHAR length differs");
      }
      return;
    }
    if (logicalType instanceof Types.FixedCharType) {
      if (!typeName.equals("char")
          || ((Types.FixedCharType) logicalType).length() != singleTypeParameter(rawTypeName)) {
        throw incompatible(identifier, "CHAR length differs");
      }
      return;
    }
    if (typeName.equals("varchar") || typeName.equals("char")) {
      throw incompatible(identifier, "logical character type differs");
    }
  }

  private static boolean isAlwaysNormalizedType(String typeName) {
    return typeName.equals("datetime")
        || typeName.equals("datetimev2")
        || typeName.equals("binary")
        || typeName.equals("varbinary")
        || typeName.equals("array")
        || typeName.equals("map")
        || typeName.equals("struct")
        || typeName.equals("largeint")
        || typeName.equals("bitmap")
        || typeName.equals("hll")
        || typeName.equals("json")
        || typeName.equals("jsonb")
        || typeName.equals("variant")
        || typeName.equals("ipv4")
        || typeName.equals("ipv6")
        || isKnownUnsignedType(typeName)
        || typeName.equals("decimal256");
  }

  private static boolean isDecimalType(String typeName) {
    return typeName.equals("decimal")
        || typeName.equals("decimalv2")
        || typeName.equals("decimal32")
        || typeName.equals("decimal64")
        || typeName.equals("decimal128")
        || typeName.equals("decimal256");
  }

  private static boolean isKnownUnsignedType(String typeName) {
    return typeName.equals("tinyint unsigned")
        || typeName.equals("smallint unsigned")
        || typeName.equals("int unsigned")
        || typeName.equals("bigint unsigned");
  }

  private static boolean isSafeDecimalFallback(String rawTypeName) {
    if (rawTypeName == null || !isDecimalType(baseType(rawTypeName))) {
      return false;
    }
    String normalized = rawTypeName.trim();
    int openingParenthesis = normalized.indexOf('(');
    if (openingParenthesis < 0 || !normalized.endsWith(")")) {
      return false;
    }
    String[] parameters =
        normalized.substring(openingParenthesis + 1, normalized.length() - 1).split(",", -1);
    if (parameters.length != 2) {
      return false;
    }
    try {
      int precision = Integer.parseInt(parameters[0].trim());
      int scale = Integer.parseInt(parameters[1].trim());
      return precision > MAX_CATALYST_DECIMAL_PRECISION && scale >= 0 && scale <= precision;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  private static int singleTypeParameter(String rawTypeName) {
    if (rawTypeName == null) {
      return -1;
    }
    String normalized = rawTypeName.trim();
    int openingParenthesis = normalized.indexOf('(');
    if (openingParenthesis < 0 || !normalized.endsWith(")")) {
      return -1;
    }
    try {
      return Integer.parseInt(
          normalized.substring(openingParenthesis + 1, normalized.length() - 1).trim());
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private static String normalizationProjection(String columnName, String rawTypeName) {
    String quoted = DorisReadSchema35.quoteIdentifier(columnName);
    String typeName = baseType(rawTypeName);
    String expression;
    if (typeName.equals("binary") || typeName.equals("varbinary")) {
      expression = "TO_BASE64(" + quoted + ")";
    } else if (typeName.equals("bitmap")) {
      expression = "BITMAP_TO_BASE64(" + quoted + ")";
    } else if (typeName.equals("hll")) {
      expression = "HLL_TO_BASE64(" + quoted + ")";
    } else {
      expression = "CAST(" + quoted + " AS STRING)";
    }
    // JDBCTable selects the visible field names from the subquery. Keep aliases on every
    // normalized expression so the outer JDBC projection remains valid.
    return expression + " AS " + quoted;
  }

  private static String baseType(String value) {
    if (value == null) {
      return "";
    }
    String normalized = value.trim().toLowerCase(Locale.ROOT);
    int parenthesis = normalized.indexOf('(');
    return parenthesis < 0 ? normalized : normalized.substring(0, parenthesis).trim();
  }

  private static String canonicalTypeName(String value) {
    return value == null ? "" : value.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", "");
  }

  private static int datetimePrecision(String rawTypeName) {
    if (rawTypeName == null) {
      return -1;
    }
    String normalized = rawTypeName.trim();
    int openingParenthesis = normalized.indexOf('(');
    if (openingParenthesis < 0) {
      return 0;
    }
    if (!normalized.endsWith(")")) {
      return -1;
    }
    try {
      return Integer.parseInt(
          normalized.substring(openingParenthesis + 1, normalized.length() - 1).trim());
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private static String expectedUnsignedType(Type logicalType) {
    if (logicalType instanceof Types.ByteType) {
      return "tinyint unsigned";
    }
    if (logicalType instanceof Types.ShortType) {
      return "smallint unsigned";
    }
    if (logicalType instanceof Types.IntegerType) {
      return "int unsigned";
    }
    if (logicalType instanceof Types.LongType) {
      return "bigint unsigned";
    }
    return "";
  }

  private static IllegalArgumentException incompatible(Identifier identifier, String reason) {
    return new IllegalArgumentException(
        String.format(Locale.ROOT, "Doris schema mismatch for %s: %s", identifier, reason));
  }
}
