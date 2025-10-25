/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.lance.common;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

public class TestArrowIPC {

  private static final String FILENAME = "/tmp/initial_data.arrow";
  private static final int RECORD_COUNT = 3;

  @Test
  void testIPC() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

      Schema schema =
          new Schema(
              Arrays.asList(
                  Field.nullable("id", new ArrowType.Int(32, true)),
                  Field.nullable("value", new ArrowType.Utf8())));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        IntVector idVector = (IntVector) root.getVector("id");
        VarCharVector valueVector = (VarCharVector) root.getVector("value");

        idVector.allocateNew();
        valueVector.allocateNew();

        for (int i = 0; i < RECORD_COUNT; i++) {
          idVector.setSafe(i, i + 1);
          valueVector.setSafe(i, new Text("Row_" + (i + 1)));
        }

        idVector.setValueCount(RECORD_COUNT);
        valueVector.setValueCount(RECORD_COUNT);
        root.setRowCount(RECORD_COUNT);

        File outFile = new File(FILENAME);
        try (FileOutputStream fos = new FileOutputStream(outFile);
            ArrowStreamWriter writer = new ArrowStreamWriter(root, null, fos)) {

          writer.start();
          writer.writeBatch();
          writer.end();
        }

        System.out.println(
            "✅ Successfully generated Arrow IPC Stream file: " + outFile.getAbsolutePath());
        System.out.println("--- Ready for cURL test ---");
      }
    }
  }
}
