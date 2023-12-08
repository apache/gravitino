/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Convert Gravitino types to iceberg types.
 *
 * <p>Referred from core/src/main/java/org/apache/iceberg/spark/SparkTypeToType.java
 */
public class ToIcebergType extends ToIcebergTypeVisitor<Type> {
  private final IcebergTable root;
  private int nextId = 0;
  private boolean nullable;

  public ToIcebergType(boolean nullable) {
    this.root = null;
    this.nullable = nullable;
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

      // for new conversions, use ordinals for ids in the root struct
      int id = isRoot ? i : getNextId();
      if (field.nullable()) {
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
  public Type array(com.datastrato.gravitino.rel.types.Types.ListType array, Type elementType) {
    if (nullable) {
      return Types.ListType.ofOptional(getNextId(), elementType);
    } else {
      return Types.ListType.ofRequired(getNextId(), elementType);
    }
  }

  @Override
  public Type map(
      com.datastrato.gravitino.rel.types.Types.MapType map, Type keyType, Type valueType) {
    if (nullable) {
      return Types.MapType.ofOptional(getNextId(), getNextId(), keyType, valueType);
    } else {
      return Types.MapType.ofRequired(getNextId(), getNextId(), keyType, valueType);
    }
  }

  @Override
  public Type atomic(com.datastrato.gravitino.rel.types.Type.PrimitiveType primitive) {
    if (primitive instanceof com.datastrato.gravitino.rel.types.Types.BooleanType) {
      return Types.BooleanType.get();
    } else if (primitive instanceof com.datastrato.gravitino.rel.types.Types.ByteType
        || primitive instanceof com.datastrato.gravitino.rel.types.Types.ShortType) {
      throw new IllegalArgumentException(
          "Iceberg do not support Byte and Short Type, use Integer instead");
    } else if (primitive instanceof com.datastrato.gravitino.rel.types.Types.IntegerType) {
      return Types.IntegerType.get();
    } else if (primitive instanceof com.datastrato.gravitino.rel.types.Types.LongType) {
      return Types.LongType.get();
    } else if (primitive instanceof com.datastrato.gravitino.rel.types.Types.FloatType) {
      return Types.FloatType.get();
    } else if (primitive instanceof com.datastrato.gravitino.rel.types.Types.DoubleType) {
      return Types.DoubleType.get();
    } else if (primitive instanceof com.datastrato.gravitino.rel.types.Types.StringType) {
      return Types.StringType.get();
    } else if (primitive instanceof com.datastrato.gravitino.rel.types.Types.DateType) {
      return Types.DateType.get();
    } else if (primitive instanceof com.datastrato.gravitino.rel.types.Types.TimeType) {
      return Types.TimeType.get();
    } else if (primitive instanceof com.datastrato.gravitino.rel.types.Types.TimestampType) {
      if (((com.datastrato.gravitino.rel.types.Types.TimestampType) primitive).hasTimeZone()) {
        return Types.TimestampType.withZone();
      } else {
        return Types.TimestampType.withoutZone();
      }
    } else if (primitive instanceof com.datastrato.gravitino.rel.types.Types.DecimalType) {
      return Types.DecimalType.of(
          ((com.datastrato.gravitino.rel.types.Types.DecimalType) primitive).precision(),
          ((com.datastrato.gravitino.rel.types.Types.DecimalType) primitive).scale());
    } else if (primitive instanceof com.datastrato.gravitino.rel.types.Types.FixedType) {
      return Types.FixedType.ofLength(
          ((com.datastrato.gravitino.rel.types.Types.FixedType) primitive).length());
    } else if (primitive instanceof com.datastrato.gravitino.rel.types.Types.BinaryType) {
      return Types.BinaryType.get();
    } else if (primitive instanceof com.datastrato.gravitino.rel.types.Types.UUIDType) {
      return Types.UUIDType.get();
    }
    throw new UnsupportedOperationException("Not a supported type: " + primitive.toString());
  }
}
