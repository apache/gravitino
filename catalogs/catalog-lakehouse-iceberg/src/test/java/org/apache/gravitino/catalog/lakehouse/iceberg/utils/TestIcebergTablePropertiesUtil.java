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
package org.apache.gravitino.catalog.lakehouse.iceberg.utils;

import java.nio.file.Path;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIcebergTablePropertiesUtil {
  @TempDir Path temporaryDirectory;

  @Test
  void testDefaultSpecComesFromMetadataAndTracksEvolution() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            temporaryDirectory.toUri().toString(),
            Map.of("default-spec-id", "999"));
    Assertions.assertEquals(0, metadata.defaultSpecId());
    Assertions.assertEquals(
        "0", IcebergTable.fromIcebergTable(metadata, "events").properties().get("default-spec-id"));

    TableMetadata evolved =
        TableMetadata.buildFrom(metadata)
            .setDefaultPartitionSpec(PartitionSpec.builderFor(schema).identity("id").build())
            .build();
    Assertions.assertEquals(1, evolved.defaultSpecId());
    Assertions.assertEquals(
        "1", IcebergTable.fromIcebergTable(evolved, "events").properties().get("default-spec-id"));
  }
}
