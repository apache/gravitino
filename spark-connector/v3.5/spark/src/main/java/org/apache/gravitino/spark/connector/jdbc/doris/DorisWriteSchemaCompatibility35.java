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

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Validates the narrow, lossless schema allowlist for governed Doris batch writes. */
final class DorisWriteSchemaCompatibility35 {

  private DorisWriteSchemaCompatibility35() {}

  static Validator validate(Table logicalTable, DorisReadSchema35 readSchema, StructType input) {
    Column[] logicalColumns = logicalTable.columns();
    StructField[] targetFields = readSchema.schema().fields();
    StructField[] inputFields = input.fields();
    if (logicalColumns.length != targetFields.length || inputFields.length != targetFields.length) {
      throw incompatible("column count differs");
    }

    List<Integer> datetimeIndexes = new ArrayList<>();
    List<Integer> datetimePrecisions = new ArrayList<>();
    for (int index = 0; index < inputFields.length; index++) {
      Column logical = logicalColumns[index];
      StructField target = targetFields[index];
      StructField source = inputFields[index];
      if (!logical.name().equals(source.name()) || !target.name().equals(source.name())) {
        throw incompatible("column order or name differs at index " + index);
      }
      if (!logical.nullable() && source.nullable()) {
        throw incompatible("nullable input targets non-null column " + source.name());
      }

      String normalizedTypeName = readSchema.normalizedTypeName(target.name());
      if (normalizedTypeName == null) {
        if (!source.dataType().equals(target.dataType())) {
          throw incompatible("type differs for column " + source.name());
        }
        continue;
      }

      if (!(logical.dataType() instanceof Types.TimestampType)
          || ((Types.TimestampType) logical.dataType()).hasTimeZone()
          || !DataTypes.StringType.equals(source.dataType())
          || !isDatetimeType(normalizedTypeName)) {
        throw incompatible("normalized Doris type is not writable for column " + source.name());
      }
      Types.TimestampType timestamp = (Types.TimestampType) logical.dataType();
      int precision = timestamp.hasPrecisionSet() ? timestamp.precision() : 0;
      if (precision < 0 || precision > 6 || precision != datetimePrecision(normalizedTypeName)) {
        throw incompatible("DATETIME precision differs for column " + source.name());
      }
      datetimeIndexes.add(index);
      datetimePrecisions.add(precision);
    }
    return new Validator(datetimeIndexes, datetimePrecisions);
  }

  /** Serializable per-row checks for the certified DATETIME String write contract. */
  static final class Validator implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int[] indexes;
    private final int[] precisions;

    private Validator(List<Integer> indexes, List<Integer> precisions) {
      this.indexes = indexes.stream().mapToInt(Integer::intValue).toArray();
      this.precisions = precisions.stream().mapToInt(Integer::intValue).toArray();
    }

    static Validator none() {
      return new Validator(Collections.emptyList(), Collections.emptyList());
    }

    void validate(InternalRow row) {
      for (int index = 0; index < indexes.length; index++) {
        int fieldIndex = indexes[index];
        if (!row.isNullAt(fieldIndex)) {
          validateDatetime(row.getUTF8String(fieldIndex).toString(), precisions[index]);
        }
      }
    }

    private static void validateDatetime(String value, int precision) {
      int expectedLength = precision == 0 ? 19 : 20 + precision;
      if (value.length() != expectedLength
          || value.charAt(4) != '-'
          || value.charAt(7) != '-'
          || value.charAt(10) != ' '
          || value.charAt(13) != ':'
          || value.charAt(16) != ':'
          || (precision > 0 && value.charAt(19) != '.')) {
        throw invalidDatetime();
      }
      for (int index = 0; index < value.length(); index++) {
        if (index == 4 || index == 7 || index == 10 || index == 13 || index == 16 || index == 19) {
          continue;
        }
        char character = value.charAt(index);
        if (character < '0' || character > '9') {
          throw invalidDatetime();
        }
      }
      try {
        LocalDateTime.of(
            integer(value, 0, 4),
            integer(value, 5, 7),
            integer(value, 8, 10),
            integer(value, 11, 13),
            integer(value, 14, 16),
            integer(value, 17, 19));
      } catch (DateTimeException e) {
        throw invalidDatetime();
      }
    }

    private static int integer(String value, int start, int end) {
      int result = 0;
      for (int index = start; index < end; index++) {
        result = result * 10 + (value.charAt(index) - '0');
      }
      return result;
    }

    private static IllegalArgumentException invalidDatetime() {
      return new IllegalArgumentException(
          "Doris DATETIME input does not match the certified precision-specific format");
    }
  }

  private static IllegalArgumentException incompatible(String reason) {
    return new IllegalArgumentException(
        String.format(Locale.ROOT, "Doris write schema is incompatible: %s", reason));
  }

  private static boolean isDatetimeType(String rawTypeName) {
    String typeName = baseType(rawTypeName);
    return typeName.equals("datetime") || typeName.equals("datetimev2");
  }

  private static int datetimePrecision(String rawTypeName) {
    int openingParenthesis = rawTypeName.indexOf('(');
    if (openingParenthesis < 0) {
      return 0;
    }
    if (!rawTypeName.endsWith(")")) {
      return -1;
    }
    try {
      return Integer.parseInt(
          rawTypeName.substring(openingParenthesis + 1, rawTypeName.length() - 1).trim());
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private static String baseType(String rawTypeName) {
    String normalized = rawTypeName.trim().toLowerCase(Locale.ROOT);
    int openingParenthesis = normalized.indexOf('(');
    return openingParenthesis < 0 ? normalized : normalized.substring(0, openingParenthesis).trim();
  }
}
