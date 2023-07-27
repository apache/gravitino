/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive.converter;

import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.CHAR_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DECIMAL_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.FLOAT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TIMESTAMP_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TINYINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.VARCHAR_TYPE_NAME;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

public class FromHiveType {
  public static Type convert(String hiveType) throws IllegalArgumentException {
    TypeInfo hiveTypeInfo = getTypeInfoFromTypeString(hiveType);
    return toSubstraitType(hiveTypeInfo);
  }

  private static Type toSubstraitType(TypeInfo hiveTypeInfo) throws IllegalArgumentException {
    switch (hiveTypeInfo.getCategory()) {
      case PRIMITIVE:
        switch (hiveTypeInfo.getTypeName()) {
          case BOOLEAN_TYPE_NAME:
            return TypeCreator.NULLABLE.BOOLEAN;
          case TINYINT_TYPE_NAME:
            return TypeCreator.NULLABLE.I8;
          case SMALLINT_TYPE_NAME:
            return TypeCreator.NULLABLE.I16;
          case INT_TYPE_NAME:
            return TypeCreator.NULLABLE.I32;
          case BIGINT_TYPE_NAME:
            return TypeCreator.NULLABLE.I64;
          case FLOAT_TYPE_NAME:
            return TypeCreator.NULLABLE.FP32;
          case DOUBLE_TYPE_NAME:
            return TypeCreator.NULLABLE.FP64;
          case STRING_TYPE_NAME:
            return TypeCreator.NULLABLE.STRING;
          case DATE_TYPE_NAME:
            return TypeCreator.NULLABLE.DATE;
          case TIMESTAMP_TYPE_NAME:
            return TypeCreator.NULLABLE.TIMESTAMP;
          case DECIMAL_TYPE_NAME:
            DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) hiveTypeInfo;
            return TypeCreator.NULLABLE.decimal(
                decimalTypeInfo.precision(), decimalTypeInfo.getScale());
          case CHAR_TYPE_NAME:
            return TypeCreator.NULLABLE.fixedChar(((CharTypeInfo) hiveTypeInfo).getLength());
          case VARCHAR_TYPE_NAME:
            return TypeCreator.NULLABLE.varChar(((VarcharTypeInfo) hiveTypeInfo).getLength());
          default:
            throw new IllegalArgumentException(
                "Unknown Hive type: " + hiveTypeInfo.getQualifiedName());
        }
      case LIST:
        return TypeCreator.NULLABLE.list(
            toSubstraitType(((ListTypeInfo) hiveTypeInfo).getListElementTypeInfo()));
      case MAP:
        MapTypeInfo mapTypeInfo = (MapTypeInfo) hiveTypeInfo;
        return TypeCreator.NULLABLE.map(
            toSubstraitType(mapTypeInfo.getMapKeyTypeInfo()),
            toSubstraitType(mapTypeInfo.getMapKeyTypeInfo()));
      case STRUCT:
        return TypeCreator.NULLABLE.struct(
            ((StructTypeInfo) hiveTypeInfo)
                .getAllStructFieldTypeInfos().stream()
                    .map(FromHiveType::toSubstraitType)
                    .collect(Collectors.toList()));
      case UNION:
        return TypeCreator.NULLABLE.struct(
            ((UnionTypeInfo) hiveTypeInfo)
                .getAllUnionObjectTypeInfos().stream()
                    .map(FromHiveType::toSubstraitType)
                    .collect(Collectors.toList()));
      default:
        throw new IllegalArgumentException(
            "Unknown category of Hive type: " + hiveTypeInfo.getCategory());
    }
  }
}
