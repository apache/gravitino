/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.rel.types.Type;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Implement a type converter to convert types in Iceberg.
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
    List<com.datastrato.gravitino.rel.types.Types.StructType.Field> fieldsList = new ArrayList<>();
    List<Types.NestedField> originalFields = struct.fields();

    for (int i = 0; i < originalFields.size(); i++) {
      Types.NestedField nestedField = originalFields.get(i);
      fieldsList.add(
          com.datastrato.gravitino.rel.types.Types.StructType.Field.of(
              nestedField.name(),
              fieldResults.get(i),
              nestedField.isOptional(),
              nestedField.doc()));
    }
    return com.datastrato.gravitino.rel.types.Types.StructType.of(
        fieldsList.toArray(new com.datastrato.gravitino.rel.types.Types.StructType.Field[0]));
  }

  @Override
  public Type field(Types.NestedField field, Type fieldResult) {
    return fieldResult;
  }

  @Override
  public Type list(Types.ListType list, Type elementResult) {
    return com.datastrato.gravitino.rel.types.Types.ListType.of(
        elementResult, list.isElementOptional());
  }

  @Override
  public Type map(Types.MapType map, Type keyResult, Type valueResult) {
    return com.datastrato.gravitino.rel.types.Types.MapType.of(
        keyResult, valueResult, map.isValueOptional());
  }

  @Override
  public Type primitive(org.apache.iceberg.types.Type.PrimitiveType primitive) {
    switch (primitive.typeId()) {
      case BOOLEAN:
        return com.datastrato.gravitino.rel.types.Types.BooleanType.get();
      case INTEGER:
        return com.datastrato.gravitino.rel.types.Types.IntegerType.get();
      case LONG:
        return com.datastrato.gravitino.rel.types.Types.LongType.get();
      case FLOAT:
        return com.datastrato.gravitino.rel.types.Types.FloatType.get();
      case DOUBLE:
        return com.datastrato.gravitino.rel.types.Types.DoubleType.get();
      case DATE:
        return com.datastrato.gravitino.rel.types.Types.DateType.get();
      case TIME:
        return com.datastrato.gravitino.rel.types.Types.TimeType.get();
      case TIMESTAMP:
        Types.TimestampType ts = (Types.TimestampType) primitive;
        if (ts.shouldAdjustToUTC()) {
          return com.datastrato.gravitino.rel.types.Types.TimestampType.withoutTimeZone();
        } else {
          return com.datastrato.gravitino.rel.types.Types.TimestampType.withTimeZone();
        }
      case STRING:
        return com.datastrato.gravitino.rel.types.Types.StringType.get();
      case UUID:
        return com.datastrato.gravitino.rel.types.Types.UUIDType.get();
      case FIXED:
        Types.FixedType fixedType = (Types.FixedType) primitive;
        return com.datastrato.gravitino.rel.types.Types.FixedType.of(fixedType.length());
      case BINARY:
        return com.datastrato.gravitino.rel.types.Types.BinaryType.get();
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) primitive;
        return com.datastrato.gravitino.rel.types.Types.DecimalType.of(
            decimal.precision(), decimal.scale());
      default:
        return com.datastrato.gravitino.rel.types.Types.UnparsedType.of(primitive.typeId().name());
    }
  }
}
