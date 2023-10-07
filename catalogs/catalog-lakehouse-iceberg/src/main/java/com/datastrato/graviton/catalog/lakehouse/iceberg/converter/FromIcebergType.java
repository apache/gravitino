/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.converter;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Implement a type converter to convert types in iceberg.
 *
 * <p>Referred from core/src/main/java/org/apache/iceberg/spark/TypeToSparkType.java
 */
public class FromIcebergType extends TypeUtil.SchemaVisitor<Type> {

  public FromIcebergType() {}

  @Override
  public Type schema(Schema schema, Type structType) {
    throw new UnsupportedOperationException("Data conversion of schema type is not supported");
  }

  @Override
  public Type struct(Types.StructType struct, List<Type> fieldResults) {
    throw new UnsupportedOperationException("Data conversion of struct type is not supported");
  }

  @Override
  public Type field(Types.NestedField field, Type fieldResult) {
    return fieldResult;
  }

  @Override
  public Type list(Types.ListType list, Type elementResult) {
    if (list.isElementOptional()) {
      return TypeCreator.NULLABLE.list(elementResult);
    } else {
      return TypeCreator.REQUIRED.list(elementResult);
    }
  }

  @Override
  public Type map(Types.MapType map, Type keyResult, Type valueResult) {
    if (map.isValueOptional()) {
      return TypeCreator.NULLABLE.map(keyResult, valueResult);
    } else {
      return TypeCreator.REQUIRED.map(keyResult, valueResult);
    }
  }

  @Override
  public Type primitive(org.apache.iceberg.types.Type.PrimitiveType primitive) {
    switch (primitive.typeId()) {
      case BOOLEAN:
        return TypeCreator.NULLABLE.BOOLEAN;
      case INTEGER:
        return TypeCreator.NULLABLE.I32;
      case LONG:
        return TypeCreator.NULLABLE.I64;
      case FLOAT:
        return TypeCreator.NULLABLE.FP32;
      case DOUBLE:
        return TypeCreator.NULLABLE.FP64;
      case DATE:
        return TypeCreator.NULLABLE.DATE;
      case TIME:
        return TypeCreator.NULLABLE.TIME;
      case TIMESTAMP:
        Types.TimestampType ts = (Types.TimestampType) primitive;
        if (ts.shouldAdjustToUTC()) {
          return TypeCreator.NULLABLE.TIMESTAMP_TZ;
        } else {
          return TypeCreator.NULLABLE.TIMESTAMP;
        }
      case STRING:
        return TypeCreator.NULLABLE.STRING;
      case UUID:
        return TypeCreator.NULLABLE.UUID;
      case FIXED:
        Types.FixedType fixedType = (Types.FixedType) primitive;
        return TypeCreator.NULLABLE.fixedChar(fixedType.length());
      case BINARY:
        return TypeCreator.NULLABLE.BINARY;
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) primitive;
        return TypeCreator.NULLABLE.decimal(decimal.precision(), decimal.scale());
      default:
        throw new UnsupportedOperationException(
            "Cannot convert unknown type to Graviton: " + primitive);
    }
  }
}
