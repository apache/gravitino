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

package org.apache.gravitino.flink.connector.utils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

public class TypeUtils {

  private TypeUtils() {}

  public static Type toGravitinoType(LogicalType logicalType) {
    switch (logicalType.getTypeRoot()) {
      case VARCHAR:
        return Types.StringType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case INTEGER:
        return Types.IntegerType.get();
      case BIGINT:
        return Types.LongType.get();
      case CHAR:
        CharType charType = (CharType) logicalType;
        return Types.FixedCharType.of(charType.getLength());
      case BOOLEAN:
        return Types.BooleanType.get();
      case BINARY:
        BinaryType binaryType = (BinaryType) logicalType;
        return Types.FixedType.of(binaryType.getLength());
      case VARBINARY:
        return Types.BinaryType.get();
      case DECIMAL:
        DecimalType decimalType = (DecimalType) logicalType;
        return Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale());
      case TINYINT:
        return Types.ByteType.get();
      case SMALLINT:
        return Types.ShortType.get();
      case DATE:
        return Types.DateType.get();
      case TIME_WITHOUT_TIME_ZONE:
        return Types.TimeType.get();
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return Types.TimestampType.withoutTimeZone();
      case INTERVAL_YEAR_MONTH:
        return Types.IntervalYearType.get();
      case INTERVAL_DAY_TIME:
        return Types.IntervalDayType.get();
      case FLOAT:
        return Types.FloatType.get();
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP_WITH_TIME_ZONE:
        return Types.TimestampType.withTimeZone();
      case ARRAY:
        ArrayType arrayType = (ArrayType) logicalType;
        Type elementType = toGravitinoType(arrayType.getElementType());
        return Types.ListType.of(elementType, arrayType.isNullable());
      case MAP:
        MapType mapType = (MapType) logicalType;
        Type keyType = toGravitinoType(mapType.getKeyType());
        Type valueType = toGravitinoType(mapType.getValueType());
        return Types.MapType.of(keyType, valueType, mapType.isNullable());
      case ROW:
        RowType rowType = (RowType) logicalType;
        Types.StructType.Field[] fields =
            rowType.getFields().stream()
                .map(
                    field -> {
                      LogicalType fieldLogicalType = field.getType();
                      Type fieldType = toGravitinoType(fieldLogicalType);
                      return Types.StructType.Field.of(
                          field.getName(),
                          fieldType,
                          fieldLogicalType.isNullable(),
                          field.getDescription().orElse(null));
                    })
                .toArray(Types.StructType.Field[]::new);
        return Types.StructType.of(fields);
      case NULL:
        return Types.NullType.get();
      case MULTISET:
      case STRUCTURED_TYPE:
      case UNRESOLVED:
      case DISTINCT_TYPE:
      case RAW:
      case SYMBOL:
      default:
        throw new UnsupportedOperationException(
            "Not support type: " + logicalType.asSummaryString());
    }
  }

  public static DataType toFlinkType(Type gravitinoType) {
    switch (gravitinoType.name()) {
      case DOUBLE:
        return DataTypes.DOUBLE();
      case STRING:
        return DataTypes.STRING();
      case INTEGER:
        return DataTypes.INT();
      case LONG:
        return DataTypes.BIGINT();
      case FLOAT:
        return DataTypes.FLOAT();
      case SHORT:
        return DataTypes.SMALLINT();
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) gravitinoType;
        return DataTypes.DECIMAL(decimalType.precision(), decimalType.scale());
      case VARCHAR:
        Types.VarCharType varCharType = (Types.VarCharType) gravitinoType;
        return DataTypes.VARCHAR(varCharType.length());
      case FIXED:
        Types.FixedType fixedType = (Types.FixedType) gravitinoType;
        return DataTypes.BINARY(fixedType.length());
      case FIXEDCHAR:
        Types.FixedCharType charType = (Types.FixedCharType) gravitinoType;
        return DataTypes.CHAR(charType.length());
      case BINARY:
        return DataTypes.BYTES();
      case BYTE:
        return DataTypes.TINYINT();
      case BOOLEAN:
        return DataTypes.BOOLEAN();
      case DATE:
        return DataTypes.DATE();
      case TIMESTAMP:
        Types.TimestampType timestampType = (Types.TimestampType) gravitinoType;
        if (timestampType.hasTimeZone()) {
          return DataTypes.TIMESTAMP_LTZ();
        } else {
          return DataTypes.TIMESTAMP();
        }
      case LIST:
        Types.ListType listType = (Types.ListType) gravitinoType;
        return DataTypes.ARRAY(
            nullable(toFlinkType(listType.elementType()), listType.elementNullable()));
      case MAP:
        Types.MapType mapType = (Types.MapType) gravitinoType;
        return DataTypes.MAP(
            toFlinkType(mapType.keyType()),
            nullable(toFlinkType(mapType.valueType()), mapType.valueNullable()));
      case STRUCT:
        Types.StructType structType = (Types.StructType) gravitinoType;
        List<DataTypes.Field> fields =
            Arrays.stream(structType.fields())
                .map(
                    f -> {
                      if (f.comment() == null) {
                        return DataTypes.FIELD(
                            f.name(), nullable(toFlinkType(f.type()), f.nullable()));
                      } else {
                        return DataTypes.FIELD(
                            f.name(), nullable(toFlinkType(f.type()), f.nullable()), f.comment());
                      }
                    })
                .collect(Collectors.toList());
        return DataTypes.ROW(fields);
      case NULL:
        return DataTypes.NULL();
      case TIME:
        return DataTypes.TIME();
      case INTERVAL_YEAR:
        return DataTypes.INTERVAL(DataTypes.YEAR());
      case INTERVAL_DAY:
        return DataTypes.INTERVAL(DataTypes.DAY());
      default:
        throw new UnsupportedOperationException("Not support " + gravitinoType.toString());
    }
  }

  private static DataType nullable(DataType dataType, boolean nullable) {
    if (nullable) {
      return dataType.nullable();
    } else {
      return dataType.notNull();
    }
  }
}
