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

import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;

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
}
