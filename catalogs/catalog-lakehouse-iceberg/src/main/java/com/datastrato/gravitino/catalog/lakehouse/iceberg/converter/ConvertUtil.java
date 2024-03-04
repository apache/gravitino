/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import java.util.Arrays;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class ConvertUtil {

  /**
   * Convert the Iceberg Table to the corresponding schema information in the Iceberg.
   *
   * @param gravitinoTable Gravitino table of Iceberg.
   * @return Iceberg schema.
   */
  public static Schema toIcebergSchema(IcebergTable gravitinoTable) {
    com.datastrato.gravitino.rel.types.Types.StructType gravitinoStructType =
        toGravitinoStructType(gravitinoTable);
    Type converted =
        ToIcebergTypeVisitor.visit(gravitinoStructType, new ToIcebergType(gravitinoStructType));
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  /**
   * Convert the Gravitino type to the Iceberg type.
   *
   * @param nullable Whether the field is nullable.
   * @param gravitinoType Gravitino type.
   * @return Iceberg type.
   */
  public static Type toIcebergType(
      boolean nullable, com.datastrato.gravitino.rel.types.Type gravitinoType) {
    return ToIcebergTypeVisitor.visit(gravitinoType, new ToIcebergType(nullable));
  }

  /**
   * Convert the nested type of Iceberg to the type of gravitino.
   *
   * @param type Iceberg type of field.
   * @return Gravitino type.
   */
  public static com.datastrato.gravitino.rel.types.Type formIcebergType(Type type) {
    return TypeUtil.visit(type, new FromIcebergType());
  }

  /**
   * Convert the nested field of Iceberg to the Iceberg column.
   *
   * @param nestedField Iceberg nested field.
   * @return Gravitino iceberg column
   */
  public static IcebergColumn fromNestedField(Types.NestedField nestedField) {
    return new IcebergColumn.Builder()
        .withName(nestedField.name())
        .withNullable(nestedField.isOptional())
        .withComment(nestedField.doc())
        .withType(ConvertUtil.formIcebergType(nestedField.type()))
        .build();
  }

  /**
   * Convert the Gravitino iceberg table to the Gravitino StructType
   *
   * @param icebergTable Gravitino iceberg table
   * @return Gravitino StructType
   */
  private static com.datastrato.gravitino.rel.types.Types.StructType toGravitinoStructType(
      IcebergTable icebergTable) {
    com.datastrato.gravitino.rel.types.Types.StructType.Field[] fields =
        Arrays.stream(icebergTable.columns())
            .map(
                column ->
                    com.datastrato.gravitino.rel.types.Types.StructType.Field.of(
                        column.name(), column.dataType(), column.nullable(), column.comment()))
            .toArray(com.datastrato.gravitino.rel.types.Types.StructType.Field[]::new);
    return com.datastrato.gravitino.rel.types.Types.StructType.of(fields);
  }

  private ConvertUtil() {}
}
