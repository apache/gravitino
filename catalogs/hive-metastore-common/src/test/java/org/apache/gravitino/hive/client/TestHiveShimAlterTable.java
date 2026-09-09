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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests verifying that the Hive shims send a {@code DO_NOT_UPDATE_STATS} environment context
 * to the metastore when {@code skipStatsUpdate} is requested, and use the plain alter otherwise.
 */
class TestHiveShimAlterTable {

  private static final String CATALOG = "hive";
  private static final String DB = "db";
  private static final String TABLE = "tbl";

  /**
   * A {@link HiveShimV2} that uses a mocked metastore client instead of connecting to a real Hive
   * Metastore. The mock is created inside {@link #createMetaStoreClient(Properties)} because that
   * method is invoked from the superclass constructor, before any subclass field is initialized.
   */
  private static class MockHiveShimV2 extends HiveShimV2 {
    MockHiveShimV2() {
      super(new Properties());
    }

    @Override
    public IMetaStoreClient createMetaStoreClient(Properties properties) {
      return mock(IMetaStoreClient.class);
    }

    IMetaStoreClient metaStoreClient() {
      return client;
    }
  }

  private HiveTable testTable() {
    Map<String, String> properties = new HashMap<>();
    properties.put(HiveConstants.LOCATION, "hdfs://ns/warehouse/db.db/tbl");
    return HiveTable.builder()
        .withName(TABLE)
        .withColumns(new Column[] {Column.of("id", Types.IntegerType.get())})
        .withProperties(properties)
        .withAuditInfo(
            AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
        .withCatalogName(CATALOG)
        .withDatabaseName(DB)
        .build();
  }

  @Test
  void testSkipStatsUpdateSendsDoNotUpdateStats() throws Exception {
    MockHiveShimV2 shim = new MockHiveShimV2();
    IMetaStoreClient client = shim.metaStoreClient();

    shim.alterTable(CATALOG, DB, TABLE, testTable(), true);

    ArgumentCaptor<EnvironmentContext> captor = ArgumentCaptor.forClass(EnvironmentContext.class);
    verify(client)
        .alter_table_with_environmentContext(eq(DB), eq(TABLE), any(Table.class), captor.capture());
    Assertions.assertEquals(
        "true", captor.getValue().getProperties().get(StatsSetupConst.DO_NOT_UPDATE_STATS));
    verify(client, never()).alter_table(any(), any(), any());
  }

  @Test
  void testDefaultUsesPlainAlter() throws Exception {
    MockHiveShimV2 shim = new MockHiveShimV2();
    IMetaStoreClient client = shim.metaStoreClient();

    shim.alterTable(CATALOG, DB, TABLE, testTable(), false);

    verify(client).alter_table(eq(DB), eq(TABLE), any(Table.class));
    verify(client, never()).alter_table_with_environmentContext(any(), any(), any(), any());
  }
}
