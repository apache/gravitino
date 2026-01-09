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
package org.apache.gravitino.lance.common.utils;

import static org.apache.gravitino.lance.common.ops.gravitino.LanceDataTypeConverter.CONVERTER;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.base.Preconditions;
import com.lancedb.lance.namespace.model.JsonArrowSchema;
import com.lancedb.lance.namespace.util.JsonArrowSchemaConverter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.gravitino.rel.Column;

public class ArrowUtils {
  public static byte[] generateIpcStream(Schema arrowSchema) throws IOException {
    try (BufferAllocator allocator = new RootAllocator()) {

      // Create an empty VectorSchemaRoot with the schema
      try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
        // Allocate empty vectors (0 rows)
        root.allocateNew();
        root.setRowCount(0);

        // Write to IPC stream
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer =
            new ArrowStreamWriter(root, null, Channels.newChannel(outputStream))) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }

        return outputStream.toByteArray();
      }
    } catch (Exception e) {
      throw new IOException("Failed to create empty Arrow IPC stream: " + e.getMessage(), e);
    }
  }

  public static Schema parseArrowIpcStream(byte[] stream) {
    Schema schema;
    try (BufferAllocator allocator = new RootAllocator();
        ByteArrayInputStream bais = new ByteArrayInputStream(stream);
        ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {
      schema = reader.getVectorSchemaRoot().getSchema();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse Arrow IPC stream", e);
    }

    Preconditions.checkArgument(schema != null, "No schema found in Arrow IPC stream");
    return schema;
  }

  /**
   * Extract Gravitino Columns from Arrow Schema in Lance table
   *
   * @param arrowSchema the Arrow Schema
   * @return the list of Gravitino Columns
   */
  public static List<Column> extractColumns(Schema arrowSchema) {
    List<Column> columns = new ArrayList<>();

    for (org.apache.arrow.vector.types.pojo.Field field : arrowSchema.getFields()) {
      columns.add(
          Column.of(
              field.getName(),
              CONVERTER.toGravitino(field),
              null,
              field.isNullable(),
              false,
              DEFAULT_VALUE_NOT_SET));
    }
    return columns;
  }

  /**
   * Convert Gravitino Columns to JsonArrowSchema
   *
   * @param columns the Gravitino Columns
   * @return the JsonArrowSchema
   */
  public static JsonArrowSchema toJsonArrowSchema(Column[] columns) {
    List<Field> fields =
        Arrays.stream(columns)
            .map(col -> CONVERTER.toArrowField(col.name(), col.dataType(), col.nullable()))
            .collect(Collectors.toList());

    return JsonArrowSchemaConverter.convertToJsonArrowSchema(
        new org.apache.arrow.vector.types.pojo.Schema(fields));
  }
}
