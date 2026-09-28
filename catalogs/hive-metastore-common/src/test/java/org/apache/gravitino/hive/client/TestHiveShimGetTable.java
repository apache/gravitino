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

package org.apache.gravitino.hive.client;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.hive.converter.HiveTableConverter;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for loading Hive 2.x table schemas through {@link HiveShim}. */
class TestHiveShimGetTable {

  private static final String CATALOG = "hive";
  private static final String DB = "db";
  private static final String TABLE = "tbl";

  private static class MockHiveShim extends HiveShim {
    MockHiveShim() {
      super(HiveClientClassLoader.HiveVersion.HIVE2, new Properties());
    }

    @Override
    public IMetaStoreClient createMetaStoreClient(Properties properties) {
      return mock(IMetaStoreClient.class);
    }

    IMetaStoreClient metaStoreClient() {
      return client;
    }
  }

  @Test
  void testGetTableSkipsFieldResolutionForRegularColumns() throws Exception {
    MockHiveShim shim = new MockHiveShim();
    IMetaStoreClient client = shim.metaStoreClient();
    when(client.getTable(DB, TABLE)).thenReturn(hiveTable("string"));

    HiveTable loaded = shim.getTable(CATALOG, DB, TABLE);

    Assertions.assertEquals(Types.StringType.get(), loaded.columns()[0].dataType());
    verify(client, never()).getFields(anyString(), anyString());
  }

  @Test
  void testGetTableResolvesDerivedColumnTypes() throws Exception {
    MockHiveShim shim = new MockHiveShim();
    IMetaStoreClient client = shim.metaStoreClient();
    when(client.getTable(DB, TABLE)).thenReturn(hiveTable(HiveShim.TYPE_FROM_DESERIALIZER));
    when(client.getFields(DB, TABLE))
        .thenReturn(List.of(new FieldSchema("value", "string", "from deserializer")));

    HiveTable loaded = shim.getTable(CATALOG, DB, TABLE);

    Assertions.assertEquals(Types.StringType.get(), loaded.columns()[0].dataType());
    Assertions.assertEquals("from deserializer", loaded.columns()[0].comment());
    verify(client).getFields(DB, TABLE);
  }

  @Test
  void testReplaceDerivedColumnsRejectsUnresolvedTypes() {
    Table table = hiveTable(HiveShim.TYPE_FROM_DESERIALIZER);
    List<FieldSchema> unresolvedColumns =
        List.of(new FieldSchema("value", HiveShim.TYPE_FROM_DESERIALIZER, "from deserializer"));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> HiveShim.replaceDerivedColumns(table, unresolvedColumns));

    Assertions.assertTrue(
        exception.getMessage().contains("did not resolve SerDe-derived column types"));
  }

  private static Table hiveTable(String columnType) {
    HiveTable table =
        HiveTable.builder()
            .withName(TABLE)
            .withColumns(new Column[] {Column.of("value", Types.StringType.get())})
            .withProperties(Map.of(HiveConstants.LOCATION, "hdfs://ns/warehouse/db.db/tbl"))
            .withAuditInfo(
                AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
            .withCatalogName(CATALOG)
            .withDatabaseName(DB)
            .build();
    Table hiveTable = HiveTableConverter.toHiveTable(table);
    hiveTable.getSd().getCols().get(0).setType(columnType);
    return hiveTable;
  }
}
