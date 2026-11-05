/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.lakehouse.iceberg.converter;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import org.apache.gravitino.rel.Column;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class ConvertUtil {

  /**
   * Convert an Apache Iceberg Table to the corresponding schema information in the Iceberg.
   *
   * @param gravitinoTable Gravitino table of Iceberg.
   * @return Iceberg schema.
   */
  public static Schema toIcebergSchema(IcebergTable gravitinoTable) {
    org.apache.gravitino.rel.types.Types.StructType gravitinoStructType =
        toGravitinoStructType(gravitinoTable);
    Type converted =
        ToIcebergTypeVisitor.visit(gravitinoStructType, new ToIcebergType(gravitinoStructType));
    return applyColumnDefaults(
        converted.asNestedType().asStructType().fields(), gravitinoTable.columns());
  }

  /**
   * Convert an array of Gravitino columns to an Iceberg Schema.
   *
   * @param columns Gravitino columns.
   * @return Iceberg schema.
   */
  public static Schema toIcebergSchema(Column[] columns) {
    Preconditions.checkArgument(columns != null, "columns must not be null");
    org.apache.gravitino.rel.types.Types.StructType.Field[] fields =
        Arrays.stream(columns)
            .map(
                col ->
                    org.apache.gravitino.rel.types.Types.StructType.Field.of(
                        col.name(), col.dataType(), col.nullable(), col.comment()))
            .toArray(org.apache.gravitino.rel.types.Types.StructType.Field[]::new);
    org.apache.gravitino.rel.types.Types.StructType structType =
        org.apache.gravitino.rel.types.Types.StructType.of(fields);
    Type converted = ToIcebergTypeVisitor.visit(structType, new ToIcebergType(structType));
    return applyColumnDefaults(converted.asNestedType().asStructType().fields(), columns);
  }

  /**
   * Convert the nested field of Iceberg to the Iceberg column.
   *
   * <p>Iceberg {@code write-default} maps to {@link Column#defaultValue()} and {@code
   * initial-default} maps to {@link Column#initialDefaultValue()}.
   *
   * @param nestedField Iceberg nested field.
   * @return Gravitino Iceberg column
   */
  public static IcebergColumn fromNestedField(Types.NestedField nestedField) {
    return IcebergColumn.builder()
        .withName(nestedField.name())
        .withNullable(nestedField.isOptional())
        .withComment(nestedField.doc())
        .withType(IcebergDataTypeConverter.CONVERTER.toGravitino(nestedField.type()))
        .withDefaultValue(FromIcebergColumnDefault.writeDefault(nestedField))
        .withInitialDefaultValue(FromIcebergColumnDefault.initialDefault(nestedField))
        .build();
  }

  /**
   * Convert the Gravitino Iceberg table to the Gravitino StructType
   *
   * @param icebergTable Gravitino Iceberg table
   * @return Gravitino StructType
   */
  private static org.apache.gravitino.rel.types.Types.StructType toGravitinoStructType(
      IcebergTable icebergTable) {
    org.apache.gravitino.rel.types.Types.StructType.Field[] fields =
        Arrays.stream(icebergTable.columns())
            .map(
                column ->
                    org.apache.gravitino.rel.types.Types.StructType.Field.of(
                        column.name(), column.dataType(), column.nullable(), column.comment()))
            .toArray(org.apache.gravitino.rel.types.Types.StructType.Field[]::new);
    return org.apache.gravitino.rel.types.Types.StructType.of(fields);
  }

  private static Schema applyColumnDefaults(
      List<Types.NestedField> icebergFields, Column[] columns) {
    List<Types.NestedField> withDefaults = new ArrayList<>(icebergFields.size());
    for (int i = 0; i < icebergFields.size(); i++) {
      Types.NestedField field = icebergFields.get(i);
      Literal<?> write =
          FromIcebergColumnDefault.toIceberg(columns[i].defaultValue(), field.type());
      Literal<?> initial =
          FromIcebergColumnDefault.toIceberg(columns[i].initialDefaultValue(), field.type());
      if (write == null && initial == null) {
        withDefaults.add(field);
      } else {
        // Seed both when only one side is set (Iceberg addColumn behavior for new fields).
        withDefaults.add(
            Types.NestedField.from(field)
                .withWriteDefault(write != null ? write : initial)
                .withInitialDefault(initial != null ? initial : write)
                .build());
      }
    }
    return new Schema(withDefaults);
  }

  private ConvertUtil() {}
}
