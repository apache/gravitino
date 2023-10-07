/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.converter;

import com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergColumn;
import com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergTable;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Convert Graviton types to iceberg types.
 *
 * <p>Referred from core/src/main/java/org/apache/iceberg/spark/SparkTypeToType.java
 */
public class ToIcebergType extends ToIcebergTypeVisitor<Type> {
  private final IcebergTable root;
  private int nextId = 0;

  public ToIcebergType() {
    this.root = null;
  }

  public ToIcebergType(IcebergTable root) {
    this.root = root;
    // the root struct's fields use the first ids
    this.nextId = root.columns().length;
  }

  private int getNextId() {
    return nextId++;
  }

  @Override
  public Type struct(IcebergTable struct, List<Type> types) {
    List<IcebergColumn> fields =
        Arrays.stream(struct.columns())
            .map(column -> (IcebergColumn) column)
            .collect(Collectors.toList());
    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(fields.size());
    boolean isRoot = root == struct;

    for (int i = 0; i < fields.size(); i += 1) {
      IcebergColumn field = fields.get(i);
      Type type = types.get(i);

      int id;
      if (isRoot) {
        // for new conversions, use ordinals for ids in the root struct
        id = i;
      } else {
        id = getNextId();
      }

      if (field.isOptional()) {
        newFields.add(Types.NestedField.optional(id, field.name(), type, field.comment()));
      } else {
        newFields.add(Types.NestedField.required(id, field.name(), type, field.comment()));
      }
    }
    return Types.StructType.of(newFields);
  }

  @Override
  public Type field(IcebergColumn field, Type typeResult) {
    return typeResult;
  }

  @Override
  public Type array(io.substrait.type.Type.ListType array, Type elementType) {
    if (array.nullable()) {
      return Types.ListType.ofOptional(getNextId(), elementType);
    } else {
      return Types.ListType.ofRequired(getNextId(), elementType);
    }
  }

  @Override
  public Type map(io.substrait.type.Type.Map map, Type keyType, Type valueType) {
    if (map.nullable()) {
      return Types.MapType.ofOptional(getNextId(), getNextId(), keyType, valueType);
    } else {
      return Types.MapType.ofRequired(getNextId(), getNextId(), keyType, valueType);
    }
  }

  @Override
  public Type atomic(io.substrait.type.Type primitive) {
    if (primitive instanceof io.substrait.type.Type.Bool) {
      return Types.BooleanType.get();
    } else if (primitive instanceof io.substrait.type.Type.I8
        || primitive instanceof io.substrait.type.Type.I16
        || primitive instanceof io.substrait.type.Type.I32) {
      return Types.IntegerType.get();
    } else if (primitive instanceof io.substrait.type.Type.I64) {
      return Types.LongType.get();
    } else if (primitive instanceof io.substrait.type.Type.FP32) {
      return Types.FloatType.get();
    } else if (primitive instanceof io.substrait.type.Type.FP64) {
      return Types.DoubleType.get();
    } else if (primitive instanceof io.substrait.type.Type.Str
        || primitive instanceof io.substrait.type.Type.VarChar) {
      return Types.StringType.get();
    } else if (primitive instanceof io.substrait.type.Type.Date) {
      return Types.DateType.get();
    } else if (primitive instanceof io.substrait.type.Type.Time) {
      return Types.TimeType.get();
    } else if (primitive instanceof io.substrait.type.Type.TimestampTZ) {
      return Types.TimestampType.withZone();
    } else if (primitive instanceof io.substrait.type.Type.Timestamp) {
      return Types.TimestampType.withoutZone();
    } else if (primitive instanceof io.substrait.type.Type.Decimal) {
      return Types.DecimalType.of(
          ((io.substrait.type.Type.Decimal) primitive).precision(),
          ((io.substrait.type.Type.Decimal) primitive).scale());
    } else if (primitive instanceof io.substrait.type.Type.FixedChar) {
      return Types.FixedType.ofLength(((io.substrait.type.Type.FixedChar) primitive).length());
    } else if (primitive instanceof io.substrait.type.Type.Binary) {
      return Types.BinaryType.get();
    } else if (primitive instanceof io.substrait.type.Type.UUID) {
      return Types.UUIDType.get();
    }
    throw new UnsupportedOperationException("Not a supported type: " + primitive.toString());
  }
}
