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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.iceberg.Schema;
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
    Schema schema = new Schema(converted.asNestedType().asStructType().fields());
    return applyIdentifierFields(schema, primaryKeyColumnNames(gravitinoTable));
  }

  /**
   * Sets the Iceberg {@code identifier-field-ids} for the primary key columns, mirroring the native
   * Flink Iceberg connector's {@code FlinkSchemaUtil.freshIdentifierFieldIds}. Iceberg requires
   * identifier fields to be required (NOT NULL) columns; this constraint is enforced by the Iceberg
   * {@link Schema} constructor itself, so no nullability promotion is performed here.
   *
   * @param schema The Iceberg schema before identifier fields are applied.
   * @param primaryKeyColumns The primary key column names (empty when there is no primary key).
   * @return The Iceberg schema carrying the identifier field ids.
   */
  private static Schema applyIdentifierFields(Schema schema, List<String> primaryKeyColumns) {
    if (primaryKeyColumns.isEmpty()) {
      return schema;
    }
    Set<Integer> identifierFieldIds = new LinkedHashSet<>();
    for (String columnName : primaryKeyColumns) {
      Types.NestedField field = schema.findField(columnName);
      Preconditions.checkArgument(
          field != null, "Cannot find primary key column in table schema: %s", columnName);
      identifierFieldIds.add(field.fieldId());
    }
    return new Schema(schema.schemaId(), schema.asStruct().fields(), identifierFieldIds);
  }

  /**
   * Extracts the primary key column names from the table's {@code PRIMARY_KEY} index. Returns an
   * empty list when no index is present.
   *
   * @param gravitinoTable Gravitino table of Iceberg.
   * @return The ordered primary key column names.
   */
  private static List<String> primaryKeyColumnNames(IcebergTable gravitinoTable) {
    Index[] indexes = gravitinoTable.index();
    if (indexes == null || indexes.length == 0) {
      return Collections.emptyList();
    }
    return Arrays.stream(indexes[0].fieldNames())
        .map(fieldName -> fieldName[0])
        .collect(Collectors.toList());
  }

  /**
   * Reconstructs the Gravitino {@code PRIMARY_KEY} index from the Iceberg {@code
   * identifier-field-ids}, mirroring Paimon's {@code constructIndexesFromPrimaryKeys}. The primary
   * key columns are ordered by their position in the schema for deterministic results, since
   * Iceberg identifier fields are an unordered set.
   *
   * @param schema The Iceberg schema loaded from the catalog.
   * @return A single-element {@code PRIMARY_KEY} index array, or an empty array when the schema has
   *     no identifier fields.
   */
  public static Index[] constructIndexesFromIdentifierFields(Schema schema) {
    Set<Integer> identifierFieldIds = schema.identifierFieldIds();
    if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
      return Indexes.EMPTY_INDEXES;
    }
    String[][] fieldNames =
        schema.columns().stream()
            .filter(field -> identifierFieldIds.contains(field.fieldId()))
            .map(field -> new String[] {field.name()})
            .toArray(String[][]::new);
    return new Index[] {Indexes.primary(IcebergTable.ICEBERG_PRIMARY_KEY_INDEX_NAME, fieldNames)};
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
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  /**
   * Convert the nested field of Iceberg to the Iceberg column.
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

  private ConvertUtil() {}
}
