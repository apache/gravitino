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
package org.apache.gravitino.hive.converter;

import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.FLOAT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TIMESTAMP_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TINYINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getDecimalTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getListTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getMapTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getPrimitiveTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getUnionTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getVarcharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.gravitino.connector.DataTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

public class HiveDataTypeConverter implements DataTypeConverter<TypeInfo, String> {
  public static final HiveDataTypeConverter CONVERTER = new HiveDataTypeConverter();

  @Override
  public TypeInfo fromGravitino(Type type) {
    switch (type.name()) {
      case BOOLEAN:
        return getPrimitiveTypeInfo(BOOLEAN_TYPE_NAME);
      case BYTE:
        return getPrimitiveTypeInfo(TINYINT_TYPE_NAME);
      case SHORT:
        return getPrimitiveTypeInfo(SMALLINT_TYPE_NAME);
      case INTEGER:
        return getPrimitiveTypeInfo(INT_TYPE_NAME);
      case LONG:
        return getPrimitiveTypeInfo(BIGINT_TYPE_NAME);
      case FLOAT:
        return getPrimitiveTypeInfo(FLOAT_TYPE_NAME);
      case DOUBLE:
        return getPrimitiveTypeInfo(DOUBLE_TYPE_NAME);
      case STRING:
        return getPrimitiveTypeInfo(STRING_TYPE_NAME);
      case VARCHAR:
        return getVarcharTypeInfo(((Types.VarCharType) type).length());
      case FIXEDCHAR:
        return getCharTypeInfo(((Types.FixedCharType) type).length());
      case DATE:
        return getPrimitiveTypeInfo(DATE_TYPE_NAME);
      case TIMESTAMP:
        if (type instanceof Types.TimestampType) {
          Types.TimestampType tsType = (Types.TimestampType) type;
          // Timestamps are interpreted to be timezoneless in Hive:
          // https://hive.apache.org/docs/latest/languagemanual-types_27838462/#timestamps
          if (tsType.hasTimeZone()) {
            throw new UnsupportedOperationException(
                "Unsupported conversion: Please use the TIMESTAMP WITHOUT TIMEZONE type. TIMESTAMP WITH TIMEZONE type is not supported by Hive.");
          }
          return getPrimitiveTypeInfo(TIMESTAMP_TYPE_NAME);
        }
        throw new UnsupportedOperationException("Unknown timestamp type: " + type);
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) type;
        return getDecimalTypeInfo(decimalType.precision(), decimalType.scale());
      case BINARY:
        return getPrimitiveTypeInfo(BINARY_TYPE_NAME);
      case INTERVAL_YEAR:
        return getPrimitiveTypeInfo(INTERVAL_YEAR_MONTH_TYPE_NAME);
      case INTERVAL_DAY:
        return getPrimitiveTypeInfo(INTERVAL_DAY_TIME_TYPE_NAME);
      case LIST:
        return getListTypeInfo(fromGravitino(((Types.ListType) type).elementType()));
      case MAP:
        Types.MapType mapType = (Types.MapType) type;
        return getMapTypeInfo(fromGravitino(mapType.keyType()), fromGravitino(mapType.valueType()));
      case STRUCT:
        Types.StructType structType = (Types.StructType) type;
        List<TypeInfo> typeInfos =
            Arrays.stream(structType.fields())
                .map(
                    t -> {
                      // Hive does not support comments in struct fields. This is no way to preserve
                      // comments in struct fields when converting from Gravitino to Hive.
                      // See: https://issues.apache.org/jira/browse/HIVE-26593
                      Preconditions.checkArgument(
                          t.comment() == null,
                          "Hive does not support comments in struct fields: %s",
                          t.name());
                      return fromGravitino(t.type());
                    })
                .collect(Collectors.toList());
        List<String> names =
            Arrays.stream(structType.fields())
                .map(Types.StructType.Field::name)
                .collect(Collectors.toList());
        return getStructTypeInfo(names, typeInfos);
      case UNION:
        return getUnionTypeInfo(
            Arrays.stream(((Types.UnionType) type).types())
                .map(this::fromGravitino)
                .collect(Collectors.toList()));
      default:
        throw new UnsupportedOperationException("Unsupported conversion to Hive type: " + type);
    }
  }

  @Override
  public Type toGravitino(String hiveType) {
    return toGravitino(getTypeInfoFromTypeString(hiveType));
  }

  private Type toGravitino(TypeInfo hiveTypeInfo) {
    switch (hiveTypeInfo.getCategory()) {
      case PRIMITIVE:
        switch (hiveTypeInfo.getTypeName()) {
          case BOOLEAN_TYPE_NAME:
            return Types.BooleanType.get();
          case TINYINT_TYPE_NAME:
            return Types.ByteType.get();
          case SMALLINT_TYPE_NAME:
            return Types.ShortType.get();
          case INT_TYPE_NAME:
            return Types.IntegerType.get();
          case BIGINT_TYPE_NAME:
            return Types.LongType.get();
          case FLOAT_TYPE_NAME:
            return Types.FloatType.get();
          case DOUBLE_TYPE_NAME:
            return Types.DoubleType.get();
          case STRING_TYPE_NAME:
            return Types.StringType.get();
          case DATE_TYPE_NAME:
            return Types.DateType.get();
          case TIMESTAMP_TYPE_NAME:
            return Types.TimestampType.withoutTimeZone();
          case BINARY_TYPE_NAME:
            return Types.BinaryType.get();
          case INTERVAL_YEAR_MONTH_TYPE_NAME:
            return Types.IntervalYearType.get();
          case INTERVAL_DAY_TIME_TYPE_NAME:
            return Types.IntervalDayType.get();
          default:
            if (hiveTypeInfo instanceof CharTypeInfo) {
              return Types.FixedCharType.of(((CharTypeInfo) hiveTypeInfo).getLength());
            }

            if (hiveTypeInfo instanceof VarcharTypeInfo) {
              return Types.VarCharType.of(((VarcharTypeInfo) hiveTypeInfo).getLength());
            }

            if (hiveTypeInfo instanceof DecimalTypeInfo) {
              DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) hiveTypeInfo;
              return Types.DecimalType.of(decimalTypeInfo.precision(), decimalTypeInfo.scale());
            }

            return Types.ExternalType.of(hiveTypeInfo.getQualifiedName());
        }
      case LIST:
        return Types.ListType.nullable(
            toGravitino(((ListTypeInfo) hiveTypeInfo).getListElementTypeInfo()));
      case MAP:
        MapTypeInfo mapTypeInfo = (MapTypeInfo) hiveTypeInfo;
        return Types.MapType.valueNullable(
            toGravitino(mapTypeInfo.getMapKeyTypeInfo()),
            toGravitino(mapTypeInfo.getMapValueTypeInfo()));
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) hiveTypeInfo;
        ArrayList<String> fieldNames = structTypeInfo.getAllStructFieldNames();
        ArrayList<TypeInfo> typeInfos = structTypeInfo.getAllStructFieldTypeInfos();
        Types.StructType.Field[] fields =
            IntStream.range(0, fieldNames.size())
                .mapToObj(
                    i ->
                        Types.StructType.Field.nullableField(
                            fieldNames.get(i), toGravitino(typeInfos.get(i))))
                .toArray(Types.StructType.Field[]::new);
        return Types.StructType.of(fields);
      case UNION:
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo) hiveTypeInfo;
        return Types.UnionType.of(
            unionTypeInfo.getAllUnionObjectTypeInfos().stream()
                .map(this::toGravitino)
                .toArray(Type[]::new));
      default:
        return Types.ExternalType.of(hiveTypeInfo.getQualifiedName());
    }
  }
}
