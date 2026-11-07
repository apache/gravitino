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

package org.apache.gravitino.spark.connector.jdbc;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.connector.expressions.Cast;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.VarcharType;

/**
 * Decides which expressions a JDBC source may evaluate without changing Spark query results.
 *
 * <p>An expression stays in Spark when it involves a value whose source semantics can differ from
 * Spark: a string-like or binary column (also inside arrays, maps, and structs), a string or binary
 * literal or cast, or a {@code TIMESTAMP_NTZ} literal the dialect cannot write in SQL. Aggregates
 * also stay in Spark for columns whose reader type differs from the type Spark sees, because the
 * pushed aggregate result is typed by the reader.
 */
class SparkJdbcPushDownPolicy {

  private final Set<String> sensitiveColumns;
  private final Set<String> readerTypeMismatchColumns;
  private final boolean ntzLiteralSupported;

  SparkJdbcPushDownPolicy(
      Column[] columns, StructType readerSchema, StructType sparkSchema, JdbcDialect dialect) {
    this.sensitiveColumns = new HashSet<>();
    for (Column column : columns) {
      if (isSensitive(column.dataType())) {
        sensitiveColumns.add(normalize(column.name()));
      }
    }
    this.readerTypeMismatchColumns = new HashSet<>();
    for (StructField field : sparkSchema.fields()) {
      DataType readerType = fieldType(readerSchema, field.name());
      // catalogString ignores nullability, which does not change pushed results.
      if (readerType == null
          || !readerType.catalogString().equals(field.dataType().catalogString())) {
        readerTypeMismatchColumns.add(normalize(field.name()));
      }
    }
    // Spark 3.5 leaves LocalDateTime literals unquoted, while Spark 4 quotes them. Check the
    // dialect itself instead of a Spark version or provider name.
    this.ntzLiteralSupported =
        dialect.compileValue(LocalDateTime.of(2000, 1, 1, 0, 0)) instanceof String;
  }

  /**
   * Returns whether a predicate, sort, or group-by expression may be evaluated by the source.
   *
   * @param expression the expression to check
   * @return true if pushing it keeps Spark results
   */
  boolean canPush(Expression expression) {
    if (expression instanceof NamedReference) {
      return !sensitiveColumns.contains(columnName((NamedReference) expression));
    }
    if (expression instanceof Literal) {
      DataType type = ((Literal<?>) expression).dataType();
      return !isSensitive(type) && (ntzLiteralSupported || !containsNtz(type));
    }
    if (expression instanceof Cast && isSensitive(((Cast) expression).dataType())) {
      return false;
    }
    for (Expression child : expression.children()) {
      if (!canPush(child)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns whether an aggregate function or group-by expression may be evaluated by the source.
   *
   * @param expression the expression to check
   * @return true if pushing it keeps Spark results and result types
   */
  boolean canPushAggregate(Expression expression) {
    if (expression instanceof NamedReference
        && readerTypeMismatchColumns.contains(columnName((NamedReference) expression))) {
      return false;
    }
    if (!canPush(expression)) {
      return false;
    }
    for (Expression child : expression.children()) {
      if (!canPushAggregate(child)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isSensitive(Type type) {
    if (type instanceof Types.ListType) {
      return isSensitive(((Types.ListType) type).elementType());
    }
    if (type instanceof Types.MapType) {
      return isSensitive(((Types.MapType) type).keyType())
          || isSensitive(((Types.MapType) type).valueType());
    }
    if (type instanceof Types.StructType) {
      for (Types.StructType.Field field : ((Types.StructType) type).fields()) {
        if (isSensitive(field.type())) {
          return true;
        }
      }
      return false;
    }
    return type instanceof Types.StringType
        || type instanceof Types.VarCharType
        || type instanceof Types.FixedCharType
        || type instanceof Types.BinaryType
        || type instanceof Types.ExternalType
        || type instanceof Types.UUIDType;
  }

  private static boolean isSensitive(DataType type) {
    if (type instanceof ArrayType) {
      return isSensitive(((ArrayType) type).elementType());
    }
    if (type instanceof MapType) {
      return isSensitive(((MapType) type).keyType()) || isSensitive(((MapType) type).valueType());
    }
    if (type instanceof StructType) {
      for (StructField field : ((StructType) type).fields()) {
        if (isSensitive(field.dataType())) {
          return true;
        }
      }
      return false;
    }
    return type instanceof StringType
        || type instanceof VarcharType
        || type instanceof CharType
        || type instanceof BinaryType;
  }

  private static boolean containsNtz(DataType type) {
    if (type instanceof ArrayType) {
      return containsNtz(((ArrayType) type).elementType());
    }
    if (type instanceof MapType) {
      return containsNtz(((MapType) type).keyType()) || containsNtz(((MapType) type).valueType());
    }
    if (type instanceof StructType) {
      for (StructField field : ((StructType) type).fields()) {
        if (containsNtz(field.dataType())) {
          return true;
        }
      }
      return false;
    }
    return type instanceof TimestampNTZType;
  }

  private static DataType fieldType(StructType schema, String name) {
    for (StructField field : schema.fields()) {
      if (field.name().equalsIgnoreCase(name)) {
        return field.dataType();
      }
    }
    return null;
  }

  private static String columnName(NamedReference reference) {
    String[] names = reference.fieldNames();
    return names.length == 0 ? "" : normalize(names[0]);
  }

  private static String normalize(String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}
