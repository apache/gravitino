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
package org.apache.gravitino.catalog.lakehouse.hudi.backend.hms;

import static org.apache.gravitino.catalog.lakehouse.hudi.HudiCatalogPropertiesMetadata.URI;
import static org.apache.gravitino.catalog.lakehouse.hudi.HudiSchemaPropertiesMetadata.LOCATION;

import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.lakehouse.hudi.HudiColumn;
import org.apache.gravitino.catalog.lakehouse.hudi.HudiSchema;
import org.apache.gravitino.catalog.lakehouse.hudi.HudiTable;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.hive.HiveClientPool;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.hive.client.HiveClient;
import org.apache.gravitino.hive.hms.MiniHiveMetastoreService;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestHudiHMSBackendOps extends MiniHiveMetastoreService {

  private static final HudiHMSBackendOps ops = new HudiHMSBackendOps();
  private static final String METALAKE_NAME = "metalake";
  private static final String CATALOG_NAME = "catalog";
  private static final String HIVE_TABLE_NAME = "hive_table";
  private static final String HUDI_TABLE_NAME = "hudi_table";
  private static final String CATALOG = "";
  private static HiveClientPool hiveClientPool;

  @BeforeAll
  public static void prepare() throws Exception {
    String hiveMetastoreUris = hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname);

    Map<String, String> props = Maps.newHashMap();
    props.put(URI, hiveMetastoreUris);
    ops.initialize(props);

    Properties properties = new Properties();
    properties.setProperty(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastoreUris);
    hiveClientPool = new HiveClientPool(1, properties);

    // create a hive table using HiveClient
    createHiveTable();

    // use Spark to create a hudi table
    SparkSession sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Hudi Catalog integration test")
            .config("hive.metastore.uris", hiveMetastoreUris)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
            .config("dfs.replication", "1")
            .enableHiveSupport()
            .getOrCreate();

    // create a hudi table
    sparkSession.sql(
        String.format("CREATE TABLE %s.%s (ts BIGINT) USING HUDI", DB_NAME, HUDI_TABLE_NAME));
  }

  private static void createHiveTable() throws Exception {
    Column col = Column.of("col1", Types.StringType.get(), "description");

    HiveTable hiveTable =
        HiveTable.builder()
            .withName(HIVE_TABLE_NAME)
            .withColumns(new Column[] {col})
            .withComment("description")
            .withProperties(new HashMap<>())
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(System.getProperty("user.name", "gravitino"))
                    .withCreateTime(Instant.now())
                    .build())
            .withCatalogName(CATALOG)
            .withDatabaseName(DB_NAME)
            .build();

    hiveClientPool.run(
        (HiveClient client) -> {
          client.createTable(hiveTable);
          return null;
        });
  }

  @AfterAll
  public static void cleanup() throws Exception {
    if (hiveClientPool != null) {
      hiveClientPool.close();
    }
    ops.close();
  }

  @Test
  public void testInitialize() {
    try (HudiHMSBackendOps ops = new HudiHMSBackendOps()) {
      String hiveMetastoreUris = hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname);
      Map<String, String> props = Maps.newHashMap();
      props.put(URI, hiveMetastoreUris);
      ops.initialize(props);
      Assertions.assertNotNull(ops.clientPool);
    }
  }

  @Test
  public void testLoadSchema() {
    HudiSchema hudiSchema = ops.loadSchema(NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DB_NAME));

    Assertions.assertEquals(DB_NAME, hudiSchema.name());
    Assertions.assertEquals("description", hudiSchema.comment());
    Assertions.assertNotNull(hudiSchema.properties().get(LOCATION));
  }

  @Test
  public void testListSchemas() {
    Namespace namespace = Namespace.of(METALAKE_NAME, CATALOG_NAME);
    NameIdentifier[] schemas = ops.listSchemas(namespace);

    Assertions.assertTrue(schemas.length > 0);
    Assertions.assertTrue(Arrays.stream(schemas).anyMatch(schema -> schema.name().equals(DB_NAME)));
  }

  @Test
  public void testListTables() {
    Namespace namespace = Namespace.of(METALAKE_NAME, CATALOG_NAME, DB_NAME);
    NameIdentifier[] tables = ops.listTables(namespace);

    // all hive tables are filtered out
    Assertions.assertEquals(1, tables.length);
    Assertions.assertEquals(HUDI_TABLE_NAME, tables[0].name());
  }

  @Test
  public void testLoadTable() {
    Namespace namespace = Namespace.of(METALAKE_NAME, CATALOG_NAME, DB_NAME);
    Exception exception =
        Assertions.assertThrows(
            NoSuchTableException.class,
            () -> ops.loadTable(NameIdentifier.of(namespace, HIVE_TABLE_NAME)));
    Assertions.assertEquals(
        "Table hive_table is not a Hudi table in Hive Metastore", exception.getMessage());

    HudiTable hudiTable = ops.loadTable(NameIdentifier.of(namespace, HUDI_TABLE_NAME));
    Assertions.assertEquals(HUDI_TABLE_NAME, hudiTable.name());
    Assertions.assertNull(hudiTable.comment());
    Assertions.assertNotNull(hudiTable.properties().get(LOCATION));

    Column[] columns = hudiTable.columns();
    Assertions.assertEquals(6, columns.length);

    Assertions.assertEquals(
        HudiColumn.builder()
            .withName("_hoodie_commit_time")
            .withType(Types.StringType.get())
            .build(),
        columns[0]);
    Assertions.assertEquals(
        HudiColumn.builder()
            .withName("_hoodie_commit_seqno")
            .withType(Types.StringType.get())
            .build(),
        columns[1]);
    Assertions.assertEquals(
        HudiColumn.builder()
            .withName("_hoodie_record_key")
            .withType(Types.StringType.get())
            .build(),
        columns[2]);
    Assertions.assertEquals(
        HudiColumn.builder()
            .withName("_hoodie_partition_path")
            .withType(Types.StringType.get())
            .build(),
        columns[3]);
    Assertions.assertEquals(
        HudiColumn.builder().withName("_hoodie_file_name").withType(Types.StringType.get()).build(),
        columns[4]);
    Assertions.assertEquals(
        HudiColumn.builder().withName("ts").withType(Types.LongType.get()).build(), columns[5]);
  }
}
