/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import com.datastrato.gravitino.rel.Column;
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
   * Traverse the gravitino table and convert the fields into iceberg fields.
   *
   * @param table iceberg table.
   * @param visitor
   * @param <T>
   * @return
   */
  public static <T> T visit(IcebergTable table, ToIcebergTypeVisitor<T> visitor) {
    Column[] columns = table.columns();
    List<T> fieldResults = Lists.newArrayListWithExpectedSize(columns.length);

    for (Column field : columns) {
      fieldResults.add(visitor.field((IcebergColumn) field, visit(field.dataType(), visitor)));
    }
    return visitor.struct(table, fieldResults);
  }

  /**
   * Convert the type mapping of gravitino to Iceberg.
   *
   * @param type TODO Abstract a data type in a gravitino.
   * @param visitor
   * @return
   * @param <T>
   */
  public static <T> T visit(Type type, ToIcebergTypeVisitor<T> visitor) {
    if (type instanceof Types.MapType) {
      Types.MapType map = (Types.MapType) type;
      return visitor.map(map, visit(map.keyType(), visitor), visit(map.valueType(), visitor));
    } else if (type instanceof Types.ListType) {
      Types.ListType list = (Types.ListType) type;
      return visitor.array(list, visit(list.elementType(), visitor));
    } else {
      return visitor.atomic((Type.PrimitiveType) type);
    }
  }

  public T struct(IcebergTable struct, List<T> fieldResults) {
    throw new UnsupportedOperationException();
  }

  public T field(IcebergColumn field, T typeResult) {
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
