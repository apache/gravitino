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
import org.apache.spark.sql.connector.expressions.aggregate.Count;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.VarcharType;

/**
 * Decides which expressions a JDBC source may evaluate without changing Spark query results.
 *
 * <p>An expression stays in Spark when it has a known difference from Spark. These are:
 *
 * <ul>
 *   <li>string-like or binary columns, also inside arrays, maps, and structs;
 *   <li>string or binary literals and casts;
 *   <li>casts to a timestamp type, and casts or date parts of a Spark {@code TIMESTAMP} column;
 *   <li>date parts other than year, quarter, month, day, hour, and minute;
 *   <li>{@code TIMESTAMP_NTZ} literals the dialect cannot write in SQL.
 * </ul>
 *
 * <p>Null checks and non-distinct counts on a column itself are exact unless its type contains an
 * external type, whose source NULL semantics may differ. Simple positive PostgreSQL/MySQL string
 * predicates can prefilter rows while Spark rechecks them; see {@link #canPrefilter(Predicate)}.
 *
 * <p>Value-dependent aggregates also stay in Spark for timestamp columns whose reader type differs
 * from the type Spark sees, because the reader types the pushed aggregate result.
 */
class SparkJdbcPushDownPolicy {

  private final Set<String> sensitiveColumns;
  private final Set<String> nullPreservingColumns;
  private final Set<String> stringColumns;
  private final boolean stringPrefilterSupported;
  private final Set<String> readerTypeMismatchColumns;
  private final Set<String> zonedTimestampColumns;
  private final boolean ntzLiteralSupported;

  SparkJdbcPushDownPolicy(
      Column[] columns,
      StructType readerSchema,
      StructType sparkSchema,
      JdbcDialect dialect,
      String jdbcUrl) {
    this.sensitiveColumns = new HashSet<>();
    this.nullPreservingColumns = new HashSet<>();
    this.stringColumns = new HashSet<>();
    String url = jdbcUrl.toLowerCase(Locale.ROOT);
    // These sources can return extra collation matches, which Spark removes with a residual.
    // Other JDBC sources retain their existing conservative policy until validated separately.
    this.stringPrefilterSupported =
        url.startsWith("jdbc:postgresql:") || url.startsWith("jdbc:mysql:");
    Set<String> externalColumns = new HashSet<>();
    Set<String> columnNames = new HashSet<>();
    Set<String> ambiguousColumns = new HashSet<>();
    for (Column column : columns) {
      if (!columnNames.add(normalize(column.name()))) {
        ambiguousColumns.add(normalize(column.name()));
      }
      Type type = column.dataType();
      if (containsExternal(type)) {
        externalColumns.add(normalize(column.name()));
      } else {
        nullPreservingColumns.add(normalize(column.name()));
      }
      if ((type instanceof Types.StringType
              || type instanceof Types.VarCharType
              || type instanceof Types.FixedCharType)
          && DataTypes.StringType.equals(JdbcSchemaUtils.fieldType(sparkSchema, column.name()))) {
        stringColumns.add(normalize(column.name()));
      }
      if (isSensitive(column.dataType())) {
        sensitiveColumns.add(normalize(column.name()));
      }
    }
    // Case-insensitive matching must not let a safe column mask a same-spelling external one.
    nullPreservingColumns.removeAll(externalColumns);
    nullPreservingColumns.removeAll(ambiguousColumns);
    stringColumns.removeAll(ambiguousColumns);
    this.readerTypeMismatchColumns = new HashSet<>();
    this.zonedTimestampColumns = new HashSet<>();
    for (StructField field : sparkSchema.fields()) {
      if (field.dataType() instanceof TimestampType) {
        zonedTimestampColumns.add(normalize(field.name()));
      }
      DataType readerType = JdbcSchemaUtils.fieldType(readerSchema, field.name());
      // Only timestamp differences change pushed aggregate results; Spark widens numeric results
      // safely. catalogString ignores nullability, which does not change pushed results.
      if (readerType == null
          || (!readerType.catalogString().equals(field.dataType().catalogString())
              && (containsTimestamp(readerType) || containsTimestamp(field.dataType())))) {
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
    if (expression instanceof Predicate) {
      Predicate predicate = (Predicate) expression;
      Expression[] children = predicate.children();
      if (("IS_NULL".equals(predicate.name()) || "IS_NOT_NULL".equals(predicate.name()))
          && children.length == 1
          && isColumn(children[0], nullPreservingColumns)) {
        // External values may be read as strings but have different source NULL semantics:
        // PostgreSQL composite IS NULL/IS NOT NULL also examines the fields of the row.
        return true;
      }
    }
    if (expression instanceof NamedReference) {
      return !sensitiveColumns.contains(columnName((NamedReference) expression));
    }
    if (expression instanceof Literal) {
      DataType type = ((Literal<?>) expression).dataType();
      return !isSensitive(type) && (ntzLiteralSupported || !containsNtz(type));
    }
    if (expression instanceof Cast) {
      Cast cast = (Cast) expression;
      // Casts to or from TIMESTAMP depend on the time zone: Spark uses the session time zone, and
      // the source uses its own. Casts to TIMESTAMP_NTZ also stay in Spark, which is conservative.
      if (isSensitive(cast.dataType())
          || containsTimestamp(cast.dataType())
          || referencesZonedTimestamp(cast.expression())) {
        return false;
      }
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
    if (expression instanceof Count) {
      Count count = (Count) expression;
      // A non-distinct count only uses nullness, not collation or the reader's value type.
      if (!count.isDistinct() && isColumn(count.column(), nullPreservingColumns)) {
        return true;
      }
    }
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

  /**
   * Returns whether a positive string predicate can reduce source rows while Spark rechecks it.
   * Negation and compound expressions are deliberately excluded: a source must not remove a row
   * that Spark would accept. This is a prefilter, never proof that the source result is exact.
   */
  boolean canPrefilter(Predicate predicate) {
    if (!stringPrefilterSupported) {
      return false;
    }
    Expression[] children = predicate.children();
    boolean equality = "=".equals(predicate.name()) && children.length == 2;
    boolean in = "IN".equals(predicate.name()) && children.length >= 2;
    if ((!equality && !in) || !isColumn(children[0], stringColumns)) {
      return false;
    }
    // A non-binary Spark collation could accept rows a stricter source would remove.
    for (int i = 1; i < children.length; i++) {
      if (!(children[i] instanceof Literal)) {
        return false;
      }
      Literal<?> literal = (Literal<?>) children[i];
      if (!DataTypes.StringType.equals(literal.dataType()) || literal.value() == null) {
        return false;
      }
      String value = literal.value().toString();
      // MySQL's dialect does not escape backslashes. PostgreSQL rejects NUL in text literals.
      if (value.indexOf('\\') >= 0 || value.indexOf('\0') >= 0) {
        return false;
      }
    }
    return true;
  }

  private static boolean isColumn(Expression expression, Set<String> names) {
    if (!(expression instanceof NamedReference)) {
      return false;
    }
    NamedReference reference = (NamedReference) expression;
    return reference.fieldNames().length == 1 && names.contains(columnName(reference));
  }

  boolean referencesZonedTimestamp(Expression expression) {
    if (expression instanceof NamedReference) {
      return zonedTimestampColumns.contains(columnName((NamedReference) expression));
    }
    for (Expression child : expression.children()) {
      if (referencesZonedTimestamp(child)) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsExternal(Type type) {
    if (type instanceof Types.ListType) {
      return containsExternal(((Types.ListType) type).elementType());
    }
    if (type instanceof Types.MapType) {
      return containsExternal(((Types.MapType) type).keyType())
          || containsExternal(((Types.MapType) type).valueType());
    }
    if (type instanceof Types.StructType) {
      for (Types.StructType.Field field : ((Types.StructType) type).fields()) {
        if (containsExternal(field.type())) {
          return true;
        }
      }
      return false;
    }
    return type instanceof Types.ExternalType;
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

  private static boolean containsTimestamp(DataType type) {
    return containsNtz(type) || contains(type, TimestampType.class);
  }

  private static boolean containsNtz(DataType type) {
    return contains(type, TimestampNTZType.class);
  }

  private static boolean contains(DataType type, Class<? extends DataType> leaf) {
    if (type instanceof ArrayType) {
      return contains(((ArrayType) type).elementType(), leaf);
    }
    if (type instanceof MapType) {
      return contains(((MapType) type).keyType(), leaf)
          || contains(((MapType) type).valueType(), leaf);
    }
    if (type instanceof StructType) {
      for (StructField field : ((StructType) type).fields()) {
        if (contains(field.dataType(), leaf)) {
          return true;
        }
      }
      return false;
    }
    return leaf.isInstance(type);
  }

  private static String columnName(NamedReference reference) {
    String[] names = reference.fieldNames();
    return names.length == 0 ? "" : normalize(names[0]);
  }

  private static String normalize(String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}
