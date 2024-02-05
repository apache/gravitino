/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.Lists;
import java.util.List;

/**
 * Type converter belonging to gravitino.
 *
 * <p>Referred from core/src/main/java/org/apache/iceberg/spark/SparkTypeVisitor.java
 */
public class ToIcebergTypeVisitor<T> {

  /**
   * Traverse the Gravitino data type and convert the fields into Iceberg fields.
   *
   * @param type Gravitino a data type in a gravitino.
   * @param visitor Visitor of Iceberg type
   * @param <T> Iceberg type
   * @return Iceberg type
   */
  public static <T> T visit(Type type, ToIcebergTypeVisitor<T> visitor) {
    if (type instanceof Types.MapType) {
      Types.MapType map = (Types.MapType) type;
      return visitor.map(map, visit(map.keyType(), visitor), visit(map.valueType(), visitor));
    } else if (type instanceof Types.ListType) {
      Types.ListType list = (Types.ListType) type;
      return visitor.array(list, visit(list.elementType(), visitor));
    } else if (type instanceof Types.StructType) {
      Types.StructType.Field[] fields = ((Types.StructType) type).fields();
      List<T> fieldResults = Lists.newArrayListWithExpectedSize(fields.length);
      for (Types.StructType.Field field : fields) {
        fieldResults.add(visitor.field(field, visit(field.type(), visitor)));
      }
      return visitor.struct(
          (com.datastrato.gravitino.rel.types.Types.StructType) type, fieldResults);
    } else {
      return visitor.atomic((Type.PrimitiveType) type);
    }
  }

  public T struct(IcebergTable struct, List<T> fieldResults) {
    throw new UnsupportedOperationException();
  }

  public T struct(
      com.datastrato.gravitino.rel.types.Types.StructType struct, List<T> fieldResults) {
    throw new UnsupportedOperationException();
  }

  public T field(Types.StructType.Field field, T typeResult) {
    throw new UnsupportedOperationException();
  }

  public T array(Types.ListType array, T elementResult) {
    throw new UnsupportedOperationException();
  }

  public T map(Types.MapType map, T keyResult, T valueResult) {
    throw new UnsupportedOperationException();
  }

  public T atomic(Type.PrimitiveType primitive) {
    throw new UnsupportedOperationException();
  }
}
