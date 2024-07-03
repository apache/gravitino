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
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import static com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.IcebergDataTypeConverter.CONVERTER;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import java.util.Arrays;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
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
   * Convert the nested field of Iceberg to the Iceberg column.
   *
   * @param nestedField Iceberg nested field.
   * @return Gravitino iceberg column
   */
  public static IcebergColumn fromNestedField(Types.NestedField nestedField) {
    return IcebergColumn.builder()
        .withName(nestedField.name())
        .withNullable(nestedField.isOptional())
        .withComment(nestedField.doc())
        .withType(CONVERTER.toGravitino(nestedField.type()))
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
