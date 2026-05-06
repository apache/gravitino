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

package org.apache.gravitino.flink.connector.utils;

import java.util.Collections;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDefaultCatalogCompat {

  @Test
  public void testCreateCatalogTable() {
    CatalogTable catalogTable =
        DefaultCatalogCompat.INSTANCE.createCatalogTable(
            Schema.newBuilder().column("id", DataTypes.INT()).build(),
            "comment",
            Collections.singletonList("id"),
            Collections.singletonMap("key", "value"));

    Assertions.assertEquals("comment", catalogTable.getComment());
    Assertions.assertEquals(Collections.singletonList("id"), catalogTable.getPartitionKeys());
    Assertions.assertEquals("value", catalogTable.getOptions().get("key"));
    Assertions.assertEquals(1, catalogTable.getUnresolvedSchema().getColumns().size());
  }

  @Test
  public void testSerializeCatalogTable() {
    CatalogTable catalogTable =
        DefaultCatalogCompat.INSTANCE.createCatalogTable(
            Schema.newBuilder().column("id", DataTypes.INT()).build(),
            "comment",
            Collections.emptyList(),
            Collections.singletonMap("key", "value"));
    ResolvedCatalogTable resolvedCatalogTable =
        new ResolvedCatalogTable(
            catalogTable,
            new ResolvedSchema(
                Collections.singletonList(Column.physical("id", DataTypes.INT())),
                Collections.emptyList(),
                null));

    Map<String, String> serialized =
        DefaultCatalogCompat.INSTANCE.serializeCatalogTable(resolvedCatalogTable);

    Assertions.assertEquals("id", serialized.get("schema.0.name"));
    Assertions.assertEquals("value", serialized.get("key"));
  }
}
