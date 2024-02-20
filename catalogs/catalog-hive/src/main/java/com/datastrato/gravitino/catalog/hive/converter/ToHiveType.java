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
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getDecimalTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getListTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getMapTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getPrimitiveTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getUnionTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getVarcharTypeInfo;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/** Converts Gravitino data types to corresponding Hive data types. */
public class ToHiveType {
  public static TypeInfo convert(Type type) {
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
        return getPrimitiveTypeInfo(TIMESTAMP_TYPE_NAME);
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
        return getListTypeInfo(convert(((Types.ListType) type).elementType()));
      case MAP:
        Types.MapType mapType = (Types.MapType) type;
        return getMapTypeInfo(convert(mapType.keyType()), convert(mapType.valueType()));
      case STRUCT:
        Types.StructType structType = (Types.StructType) type;
        List<TypeInfo> typeInfos =
            Arrays.stream(structType.fields())
                .map(t -> convert(t.type()))
                .collect(Collectors.toList());
        List<String> names =
            Arrays.stream(structType.fields())
                .map(Types.StructType.Field::name)
                .collect(Collectors.toList());
        return getStructTypeInfo(names, typeInfos);
      case UNION:
        return getUnionTypeInfo(
            Arrays.stream(((Types.UnionType) type).types())
                .map(ToHiveType::convert)
                .collect(Collectors.toList()));
      default:
        throw new UnsupportedOperationException("Unsupported conversion to Hive type: " + type);
    }
  }

  private ToHiveType() {}
}
