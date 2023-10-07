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
   * Convert the iceberg Table to the corresponding schema information in the iceberg.
   *
   * @param icebergTable Iceberg table.
   * @return
   */
  public static Schema toIcebergSchema(IcebergTable icebergTable) {
    Type converted = ToIcebergTypeVisitor.visit(icebergTable, new ToIcebergType(icebergTable));
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  /**
   * Convert the basic iceberg field type.
   *
   * @param primitive
   * @return
   */
  public static Type toPrimitiveType(io.substrait.type.Type primitive) {
    return ToIcebergTypeVisitor.visit(primitive, new ToIcebergType());
  }

  /**
   * Convert the nested type of iceberg to the type of graviton.
   *
   * @param type iceberg type of field.
   * @return
   */
  public static io.substrait.type.Type formIcebergType(Type type) {
    return TypeUtil.visit(type, new FormIcebergType());
  }

  /**
   * Convert the nested field. to the {@link IcebergColumn}.
   *
   * @param nestedField iceberg nested field.
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
