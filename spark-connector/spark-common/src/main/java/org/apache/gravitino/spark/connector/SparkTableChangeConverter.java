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
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

public class SparkTableChangeConverter {
  private SparkTypeConverter sparkTypeConverter;

  public SparkTableChangeConverter(SparkTypeConverter sparkTypeConverter) {
    this.sparkTypeConverter = sparkTypeConverter;
  }

  /**
   * Converts a Spark table change that does not require target column type information.
   *
   * @param change the Spark table change
   * @return the converted Gravitino table change
   * @throws IllegalArgumentException if a timestamp literal requires the target column schema
   */
  public org.apache.gravitino.rel.TableChange toGravitinoTableChange(TableChange change) {
    return toGravitinoTableChange(
        change,
        () -> {
          throw new IllegalArgumentException("Timestamp defaults require the target column schema");
        });
  }

  /**
   * Converts a Spark table change, coercing timestamp literals to the target column's time
   * semantics.
   *
   * @param change the Spark table change
   * @param columns supplies the current table columns, loaded only for timestamp literals
   * @return the converted Gravitino table change
   * @throws IllegalArgumentException if the default is not a supported literal or a timestamp's
   *     target column is missing or is not a timestamp
   * @throws UnsupportedOperationException if the table change or DROP DEFAULT is unsupported
   */
  public org.apache.gravitino.rel.TableChange toGravitinoTableChange(
      TableChange change, Supplier<Column[]> columns) {
    if (change instanceof TableChange.SetProperty) {
      TableChange.SetProperty setProperty = (TableChange.SetProperty) change;
      if (ConnectorConstants.COMMENT.equals(setProperty.property())) {
        return org.apache.gravitino.rel.TableChange.updateComment(setProperty.value());
      } else {
        return org.apache.gravitino.rel.TableChange.setProperty(
            setProperty.property(), setProperty.value());
      }
    } else if (change instanceof TableChange.RemoveProperty) {
      TableChange.RemoveProperty removeProperty = (TableChange.RemoveProperty) change;
      Preconditions.checkArgument(
          !ConnectorConstants.COMMENT.equals(removeProperty.property()),
          "Gravitino doesn't support remove table comment yet");
      return org.apache.gravitino.rel.TableChange.removeProperty(removeProperty.property());
    } else if (change instanceof TableChange.AddColumn) {
      TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
      return org.apache.gravitino.rel.TableChange.addColumn(
          addColumn.fieldNames(),
          sparkTypeConverter.toGravitinoType(addColumn.dataType()),
          addColumn.comment(),
          transformColumnPosition(addColumn.position()),
          addColumn.isNullable());
    } else if (change instanceof TableChange.DeleteColumn) {
      TableChange.DeleteColumn deleteColumn = (TableChange.DeleteColumn) change;
      return org.apache.gravitino.rel.TableChange.deleteColumn(
          deleteColumn.fieldNames(), deleteColumn.ifExists());
    } else if (change instanceof TableChange.UpdateColumnType) {
      TableChange.UpdateColumnType updateColumnType = (TableChange.UpdateColumnType) change;
      return org.apache.gravitino.rel.TableChange.updateColumnType(
          updateColumnType.fieldNames(),
          sparkTypeConverter.toGravitinoType(updateColumnType.newDataType()));
    } else if (change instanceof TableChange.RenameColumn) {
      TableChange.RenameColumn renameColumn = (TableChange.RenameColumn) change;
      return org.apache.gravitino.rel.TableChange.renameColumn(
          renameColumn.fieldNames(), renameColumn.newName());
    } else if (change instanceof TableChange.UpdateColumnPosition) {
      TableChange.UpdateColumnPosition sparkUpdateColumnPosition =
          (TableChange.UpdateColumnPosition) change;
      org.apache.gravitino.rel.TableChange.UpdateColumnPosition gravitinoUpdateColumnPosition =
          (org.apache.gravitino.rel.TableChange.UpdateColumnPosition)
              org.apache.gravitino.rel.TableChange.updateColumnPosition(
                  sparkUpdateColumnPosition.fieldNames(),
                  transformColumnPosition(sparkUpdateColumnPosition.position()));
      Preconditions.checkArgument(
          !(gravitinoUpdateColumnPosition.getPosition()
              instanceof org.apache.gravitino.rel.TableChange.Default),
          "Doesn't support alter column position without specifying position");
      return gravitinoUpdateColumnPosition;
    } else if (change instanceof TableChange.UpdateColumnComment) {
      TableChange.UpdateColumnComment updateColumnComment =
          (TableChange.UpdateColumnComment) change;
      return org.apache.gravitino.rel.TableChange.updateColumnComment(
          updateColumnComment.fieldNames(), updateColumnComment.newComment());
    } else if (change instanceof TableChange.UpdateColumnNullability) {
      TableChange.UpdateColumnNullability updateColumnNullability =
          (TableChange.UpdateColumnNullability) change;
      return org.apache.gravitino.rel.TableChange.updateColumnNullability(
          updateColumnNullability.fieldNames(), updateColumnNullability.nullable());
    } else if (change instanceof TableChange.UpdateColumnDefaultValue) {
      TableChange.UpdateColumnDefaultValue updateColumnDefaultValue =
          (TableChange.UpdateColumnDefaultValue) change;
      return org.apache.gravitino.rel.TableChange.updateColumnDefaultValue(
          updateColumnDefaultValue.fieldNames(),
          convertDefaultValue(
              updateColumnDefaultValue.newDefaultValue(),
              updateColumnDefaultValue.fieldNames(),
              columns));
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported table change %s", change.getClass().getName()));
    }
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
    if (parsed.dataType() instanceof TimestampType
        || parsed.dataType() instanceof TimestampNTZType) {
      Type target = columnType(columns.get(), fieldNames);
      Preconditions.checkArgument(
          target instanceof Types.TimestampType,
          "Timestamp defaults require a timestamp target column: %s",
          String.join(".", fieldNames));
      // Coerce in Spark's session zone before JDBC sees the value. A database connection's
      // zone must not decide how a local timestamp is assigned to an instant, or vice versa.
      ZoneId zone = DateTimeUtils.getZoneId(SQLConf.get().sessionLocalTimeZone());
      boolean sourceZoned = parsed.dataType() instanceof TimestampType;
      if (((Types.TimestampType) target).hasTimeZone()) {
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
    } else if (parsed.dataType() instanceof BinaryType) {
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

  private org.apache.gravitino.rel.TableChange.ColumnPosition transformColumnPosition(
      TableChange.ColumnPosition columnPosition) {
    if (null == columnPosition) {
      return org.apache.gravitino.rel.TableChange.ColumnPosition.defaultPos();
    } else if (columnPosition instanceof TableChange.First) {
      return org.apache.gravitino.rel.TableChange.ColumnPosition.first();
    } else if (columnPosition instanceof TableChange.After) {
      TableChange.After after = (TableChange.After) columnPosition;
      return org.apache.gravitino.rel.TableChange.ColumnPosition.after(after.column());
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported table column position %s", columnPosition.getClass().getName()));
    }
  }
}
