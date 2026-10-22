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

package org.apache.gravitino.hive;

import static org.apache.gravitino.catalog.hive.HiveConstants.INPUT_FORMAT;
import static org.apache.gravitino.catalog.hive.HiveConstants.OUTPUT_FORMAT;
import static org.apache.gravitino.catalog.hive.HiveConstants.SERDE_LIB;
import static org.apache.gravitino.catalog.hive.HiveConstants.TABLE_TYPE;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.hive.hms.MiniHiveMetastoreService;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestHiveSerDeSchema extends MiniHiveMetastoreService {
  private static final String MARKER = "<derived from deserializer>";

  /**
   * Starts Hive 2 HMS with the compatibility setting needed to preserve SerDe markers.
   *
   * @throws Exception If the metastore cannot start.
   */
  @BeforeAll
  public static void startMetastore() throws Exception {
    MiniHiveMetastoreService.startMetastore(
        Map.of("hive.metastore.disallow.incompatible.col.type.changes", "false"));
  }

  @Test
  void testRealSerDeResolutionAndMarkerPreservingAlters() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(
        HiveConf.ConfVars.METASTOREURIS.varname,
        hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));
    String name = "serde_" + genRandomName();
    HiveTable table =
        HiveTable.builder()
            .withName(name)
            .withCatalogName("hive")
            .withDatabaseName(DB_NAME)
            .withColumns(new Column[] {Column.of("value", Types.StringType.get())})
            .withProperties(
                new HashMap<>(
                    Map.of(
                        SERDE_LIB, "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                        INPUT_FORMAT, "org.apache.hadoop.mapred.TextInputFormat",
                        OUTPUT_FORMAT, "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                        TABLE_TYPE, "EXTERNAL_TABLE")))
            .withAuditInfo(
                AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
            .build();
    table.setOriginalStorageColumns(List.of(Column.of("value", Types.UnparsedType.of(MARKER))));
    try (HiveClientPool pool = new HiveClientPool("serde", 1, properties)) {
      pool.run(
          client -> {
            client.createTable(table);
            return null;
          });
      Assertions.assertEquals(
          MARKER, metastoreClient.getTable(DB_NAME, name).getSd().getCols().get(0).getType());
      HiveTable loaded = pool.run(client -> client.getTable("hive", DB_NAME, name));
      Assertions.assertEquals(Types.StringType.get(), loaded.columns()[0].dataType());
      Assertions.assertEquals(
          Types.StringType.get(),
          pool.run(client -> client.getTableObjectsByName("hive", DB_NAME, List.of(name)))
              .get(0)
              .columns()[0]
              .dataType());
      loaded.properties().put("serde_test", "value");
      pool.run(
          client -> {
            client.alterTable("hive", DB_NAME, name, loaded, true);
            return null;
          });
      Assertions.assertEquals(
          MARKER, metastoreClient.getTable(DB_NAME, name).getSd().getCols().get(0).getType());
      Assertions.assertEquals(
          "value", metastoreClient.getTable(DB_NAME, name).getParameters().get("serde_test"));
      pool.run(
          client -> {
            client.dropTable("hive", DB_NAME, name, false, false);
            return null;
          });
    }
  }
}
