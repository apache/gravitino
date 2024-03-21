/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive.converter;

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
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

/** Converts Hive data types to corresponding Gravitino data types. */
public class FromHiveType {

  /**
   * Converts a Hive data type string to the corresponding Gravitino data type.
   *
   * @param hiveType The Hive data type string to convert.
   * @return The equivalent Gravitino data type.
   */
  public static Type convert(String hiveType) {
    TypeInfo hiveTypeInfo = getTypeInfoFromTypeString(hiveType);
    return toGravitinoType(hiveTypeInfo);
  }

  /**
   * Converts a Hive TypeInfo object to the corresponding Gravitino Type.
   *
   * @param hiveTypeInfo The Hive TypeInfo object to convert.
   * @return The equivalent Gravitino Type.
   */
  @VisibleForTesting
  public static Type toGravitinoType(TypeInfo hiveTypeInfo) {
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

            return Types.UnparsedType.of(hiveTypeInfo.getQualifiedName());
        }
      case LIST:
        return Types.ListType.nullable(
            toGravitinoType(((ListTypeInfo) hiveTypeInfo).getListElementTypeInfo()));
      case MAP:
        MapTypeInfo mapTypeInfo = (MapTypeInfo) hiveTypeInfo;
        return Types.MapType.valueNullable(
            toGravitinoType(mapTypeInfo.getMapKeyTypeInfo()),
            toGravitinoType(mapTypeInfo.getMapValueTypeInfo()));
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) hiveTypeInfo;
        ArrayList<String> fieldNames = structTypeInfo.getAllStructFieldNames();
        ArrayList<TypeInfo> typeInfos = structTypeInfo.getAllStructFieldTypeInfos();
        Types.StructType.Field[] fields =
            IntStream.range(0, fieldNames.size())
                .mapToObj(
                    i ->
                        Types.StructType.Field.nullableField(
                            fieldNames.get(i), toGravitinoType(typeInfos.get(i))))
                .toArray(Types.StructType.Field[]::new);
        return Types.StructType.of(fields);
      case UNION:
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo) hiveTypeInfo;
        return Types.UnionType.of(
            unionTypeInfo.getAllUnionObjectTypeInfos().stream()
                .map(FromHiveType::toGravitinoType)
                .toArray(Type[]::new));
      default:
        return Types.UnparsedType.of(hiveTypeInfo.getQualifiedName());
    }
  }

  private FromHiveType() {}
}
