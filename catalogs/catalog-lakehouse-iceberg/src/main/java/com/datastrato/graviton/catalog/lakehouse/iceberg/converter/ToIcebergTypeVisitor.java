/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.converter;

import com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergColumn;
import com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergTable;
import com.datastrato.graviton.rel.Column;
import com.google.common.collect.Lists;
import io.substrait.type.Type;
import java.util.List;

/**
 * Type converter belonging to graviton.
 *
 * <p>Referred from core/src/main/java/org/apache/iceberg/spark/SparkTypeVisitor.java
 */
public class ToIcebergTypeVisitor<T> {

  /**
   * Traverse the graviton table and convert the fields into iceberg fields.
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
   * Convert the type mapping of graviton to iceberg.
   *
   * @param type TODO Abstract a data type in a graviton.
   * @param visitor
   * @return
   * @param <T>
   */
  public static <T> T visit(Type type, ToIcebergTypeVisitor<T> visitor) {
    if (type instanceof Type.Map) {
      Type.Map map = (Type.Map) type;
      return visitor.map(map, visit(map.key(), visitor), visit(map.value(), visitor));
    } else if (type instanceof Type.ListType) {
      Type.ListType list = (Type.ListType) type;
      return visitor.array(list, visit(list.elementType(), visitor));
    } else {
      return visitor.atomic(type);
    }
  }

  public T struct(IcebergTable struct, List<T> fieldResults) {
    throw new UnsupportedOperationException();
  }

  public T field(IcebergColumn field, T typeResult) {
    throw new UnsupportedOperationException();
  }

  public T array(Type.ListType array, T elementResult) {
    throw new UnsupportedOperationException();
  }

  public T map(Type.Map map, T keyResult, T valueResult) {
    throw new UnsupportedOperationException();
  }

  public T atomic(Type primitive) {
    throw new UnsupportedOperationException();
  }
}
