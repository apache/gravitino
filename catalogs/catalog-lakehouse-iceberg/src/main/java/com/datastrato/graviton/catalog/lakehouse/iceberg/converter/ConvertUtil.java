/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.converter;

import com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergColumn;
import com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class ConvertUtil {

  /**
   * Convert the Iceberg Table to the corresponding schema information in the Iceberg.
   *
   * @param icebergTable Iceberg table.
   * @return iceberg schema.
   */
  public static Schema toIcebergSchema(IcebergTable icebergTable) {
    Type converted = ToIcebergTypeVisitor.visit(icebergTable, new ToIcebergType(icebergTable));
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  /**
   * Convert the Graviton type to the Iceberg type.
   *
   * @param gravitonType
   * @return Iceberg type.
   */
  public static Type toIcebergType(io.substrait.type.Type gravitonType) {
    return ToIcebergTypeVisitor.visit(gravitonType, new ToIcebergType());
  }

  /**
   * Convert the nested type of Iceberg to the type of graviton.
   *
   * @param type Iceberg type of field.
   * @return
   */
  public static io.substrait.type.Type formIcebergType(Type type) {
    return TypeUtil.visit(type, new FromIcebergType());
  }

  /**
   * Convert the nested field of Iceberg to the Iceberg column.
   *
   * @param nestedField Iceberg nested field.
   * @return
   */
  public static IcebergColumn fromNestedField(Types.NestedField nestedField) {
    return new IcebergColumn.Builder()
        .withId(nestedField.fieldId())
        .withName(nestedField.name())
        .withOptional(nestedField.isOptional())
        .withComment(nestedField.doc())
        .withType(ConvertUtil.formIcebergType(nestedField.type()))
        .build();
  }
}
