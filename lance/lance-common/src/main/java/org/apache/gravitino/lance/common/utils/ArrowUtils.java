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
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
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
    return parseArrowIpcStream(stream, false);
  }

  /**
   * Parses a schema-only Arrow IPC stream, rejecting record batches containing rows.
   *
   * @param stream the Arrow IPC stream
   * @return the stream schema
   * @throws UnsupportedOperationException if any record batch contains rows
   * @throws IllegalArgumentException if the stream cannot be parsed
   */
  public static Schema parseSchemaOnlyIpcStream(byte[] stream) {
    return parseArrowIpcStream(stream, true);
  }

  private static Schema parseArrowIpcStream(byte[] stream, boolean requireEmpty) {
    Schema schema;
    boolean containsRows = false;
    try (BufferAllocator allocator = new RootAllocator();
        ByteArrayInputStream bais = new ByteArrayInputStream(stream);
        ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {
      schema = reader.getVectorSchemaRoot().getSchema();
      if (requireEmpty) {
        containsRows = containsRecordBatchRows(bais);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse Arrow IPC stream", e);
    }

    Preconditions.checkArgument(schema != null, "No schema found in Arrow IPC stream");
    if (containsRows) {
      throw new UnsupportedOperationException(
          "CreateTable only supports schema-only Arrow streams; "
              + "write records through a Lance client or engine after creation");
    }
    return schema;
  }

  private static boolean containsRecordBatchRows(ByteArrayInputStream input) throws IOException {
    // The schema reader has consumed the schema message. Inspect only subsequent message headers;
    // skipping bodies avoids allocating or decoding vectors, including dictionary values.
    try (ReadChannel channel = new ReadChannel(Channels.newChannel(input))) {
      MessageMetadataResult metadata;
      while ((metadata = MessageSerializer.readMessage(channel)) != null) {
        Message message = metadata.getMessage();
        if (message.headerType() == MessageHeader.RecordBatch) {
          RecordBatch batch = (RecordBatch) message.header(new RecordBatch());
          Preconditions.checkArgument(batch.length() >= 0, "Invalid Arrow record batch row count");
          if (batch.length() > 0) {
            return true;
          }
        } else if (message.headerType() != MessageHeader.DictionaryBatch) {
          throw new IOException("Unexpected Arrow message type: " + message.headerType());
        }
        Preconditions.checkArgument(message.bodyLength() >= 0, "Invalid Arrow message body length");
        input.skipNBytes(message.bodyLength());
      }
      return false;
    }
  }
}
