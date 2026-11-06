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
package org.apache.gravitino.spark.connector;

import com.google.common.base.Preconditions;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.function.Supplier;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.utils.SparkPartitionUtils;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.UnaryMinus;
import org.apache.spark.sql.catalyst.expressions.UnaryPositive;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

public class SparkTableChangeConverter34 extends SparkTableChangeConverter {
  public SparkTableChangeConverter34(SparkTypeConverter sparkTypeConverter) {
    super(sparkTypeConverter);
  }

  /** {@inheritDoc} */
  @Override
  public org.apache.gravitino.rel.TableChange toGravitinoTableChange(TableChange change) {
    return toGravitinoTableChange(
        change,
        () -> {
          throw new IllegalArgumentException(
              "Timestamp, date, and string defaults require the target column schema");
        });
  }

  /** {@inheritDoc} */
  @Override
  public org.apache.gravitino.rel.TableChange toGravitinoTableChange(
      TableChange change, Supplier<Column[]> columns) {
    if (change instanceof TableChange.UpdateColumnDefaultValue) {
      TableChange.UpdateColumnDefaultValue defaultValue =
          (TableChange.UpdateColumnDefaultValue) change;
      return org.apache.gravitino.rel.TableChange.updateColumnDefaultValue(
          defaultValue.fieldNames(),
          convertDefaultValue(defaultValue.newDefaultValue(), defaultValue.fieldNames(), columns));
    }
    return super.toGravitinoTableChange(change);
  }

  private Expression convertDefaultValue(
      String sql, String[] fieldNames, Supplier<Column[]> columns) {
    // Spark uses an empty SQL string for DROP DEFAULT; a string literal containing no characters
    // is represented by two quote characters instead.
    if (sql.isEmpty()) {
      throw new UnsupportedOperationException("DROP DEFAULT is not supported by Gravitino");
    }
    org.apache.spark.sql.catalyst.expressions.Expression parsed;
    Object value;
    try {
      parsed = CatalystSqlParser$.MODULE$.parseExpression(sql);
      boolean literal =
          parsed instanceof Literal
              || (parsed instanceof UnaryMinus && ((UnaryMinus) parsed).child() instanceof Literal)
              || (parsed instanceof UnaryPositive
                  && ((UnaryPositive) parsed).child() instanceof Literal);
      Preconditions.checkArgument(
          literal && parsed.resolved() && parsed.foldable(),
          "Only literal column defaults are supported: %s",
          sql);
      value = parsed.eval(null);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unsupported column default SQL: " + sql, e);
    }
    boolean timestampLiteral =
        parsed.dataType() instanceof TimestampType || parsed.dataType() instanceof TimestampNTZType;
    if (timestampLiteral
        || parsed.dataType() instanceof DateType
        || parsed.dataType() instanceof StringType) {
      Type target = columnType(columns.get(), fieldNames);
      Preconditions.checkArgument(
          !timestampLiteral || target instanceof Types.TimestampType,
          "Timestamp defaults require a timestamp target column: %s",
          String.join(".", fieldNames));
      if (target instanceof Types.TimestampType) {
        Preconditions.checkArgument(
            !(parsed.dataType() instanceof StringType),
            "String defaults for timestamp column %s are unsupported; use a TIMESTAMP_LTZ or TIMESTAMP_NTZ literal",
            String.join(".", fieldNames));
        // Coerce in Spark's session zone before JDBC sees the value. A database connection's
        // zone must not decide how a date or local timestamp is assigned to an instant.
        ZoneId zone = DateTimeUtils.getZoneId(SQLConf.get().sessionLocalTimeZone());
        boolean targetZoned = ((Types.TimestampType) target).hasTimeZone();
        if (parsed.dataType() instanceof DateType) {
          return targetZoned
              ? Literals.of(
                  DateTimeUtils.microsToInstant(DateTimeUtils.daysToMicros((int) value, zone))
                      .atOffset(ZoneOffset.UTC),
                  Types.TimestampType.withTimeZone())
              : Literals.timestampLiteral(LocalDate.ofEpochDay((int) value).atStartOfDay());
        }
        boolean sourceZoned = parsed.dataType() instanceof TimestampType;
        if (targetZoned) {
          return Literals.of(
              (sourceZoned
                      ? DateTimeUtils.microsToInstant((long) value)
                      : DateTimeUtils.microsToLocalDateTime((long) value).atZone(zone).toInstant())
                  .atOffset(ZoneOffset.UTC),
              Types.TimestampType.withTimeZone());
        }
        return Literals.timestampLiteral(
            sourceZoned
                ? DateTimeUtils.microsToInstant((long) value).atZone(zone).toLocalDateTime()
                : DateTimeUtils.microsToLocalDateTime((long) value));
      }
    }
    if (parsed.dataType() instanceof BinaryType) {
      return Literals.of((byte[]) value, Types.BinaryType.get());
    }
    return SparkPartitionUtils.toGravitinoLiteral(
        new GenericInternalRow(new Object[] {value}), 0, parsed.dataType());
  }

  private Type columnType(Column[] columns, String[] fieldNames) {
    Type type =
        Arrays.stream(columns)
            .filter(column -> column.name().equals(fieldNames[0]))
            .findFirst()
            .orElseThrow(
                () -> new IllegalArgumentException("Unknown default column: " + fieldNames[0]))
            .dataType();
    for (int i = 1; i < fieldNames.length; i++) {
      Preconditions.checkArgument(
          type instanceof Types.StructType,
          "Unsupported nested default column: %s",
          String.join(".", fieldNames));
      String fieldName = fieldNames[i];
      type =
          Arrays.stream(((Types.StructType) type).fields())
              .filter(field -> field.name().equals(fieldName))
              .findFirst()
              .orElseThrow(
                  () -> new IllegalArgumentException("Unknown default column: " + fieldName))
              .type();
    }
    return type;
  }
}
