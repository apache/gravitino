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

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestArrowUtils {

  @Test
  public void testParseArrowIpcStream() throws Exception {
    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("value", new ArrowType.Utf8())));
    byte[] ipcStream = ArrowUtils.generateIpcStream(schema);
    Schema parsedSchema = ArrowUtils.parseArrowIpcStream(ipcStream);

    Assertions.assertEquals(schema, parsedSchema);
  }
  /** Verifies schema-only streams and zero-row batches remain supported. */
  @Test
  public void testSchemaOnlyStreams() throws Exception {
    Schema expected = new Schema(List.of(Field.nullable("id", new ArrowType.Int(32, true))));
    Assertions.assertEquals(expected, ArrowUtils.parseSchemaOnlyIpcStream(streamWithRows()));
    Assertions.assertEquals(expected, ArrowUtils.parseSchemaOnlyIpcStream(streamWithRows(0, 0)));
  }

  /** Verifies that a non-empty batch is rejected, including after empty batches. */
  @Test
  public void testRejectRecordBatchesWithRows() throws Exception {
    for (byte[] stream : List.of(streamWithRows(1), streamWithRows(0, 1))) {
      UnsupportedOperationException exception =
          Assertions.assertThrows(
              UnsupportedOperationException.class,
              () -> ArrowUtils.parseSchemaOnlyIpcStream(stream));
      Assertions.assertTrue(exception.getMessage().contains("schema-only"));
      // Existing callers of the general schema parser retain their previous behavior.
      Assertions.assertEquals(1, ArrowUtils.parseArrowIpcStream(stream).getFields().size());
    }
  }

  /** Verifies malformed input is reported as invalid rather than as unsupported data. */
  @Test
  public void testRejectMalformedSchemaOnlyStream() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ArrowUtils.parseSchemaOnlyIpcStream(new byte[] {1, 2, 3}));
  }

  private byte[] streamWithRows(int... batches) throws Exception {
    Schema schema = new Schema(List.of(Field.nullable("id", new ArrowType.Int(32, true))));
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
      root.allocateNew();
      writer.start();
      for (int rows : batches) {
        for (int i = 0; i < rows; i++) {
          ((IntVector) root.getVector("id")).setSafe(i, i);
        }
        root.setRowCount(rows);
        writer.writeBatch();
      }
      writer.end();
      return output.toByteArray();
    }
  }
}
