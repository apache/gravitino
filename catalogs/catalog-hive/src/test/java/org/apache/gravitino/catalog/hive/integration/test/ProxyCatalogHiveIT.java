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
package org.apache.gravitino.catalog.hive.integration.test;

import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMeta.IMPERSONATION_ENABLE;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;
import static org.apache.gravitino.server.GravitinoServer.WEBSERVER_CONF_PREFIX;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.dto.rel.partitioning.Partitioning;
import org.apache.gravitino.hive.HiveClientPool;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class ProxyCatalogHiveIT extends BaseIT {

  public static final String METALAKE_NAME =
      GravitinoITUtils.genRandomName("ProxyCatalogHiveIT_metalake");
  public static final String CATALOG_NAME = GravitinoITUtils.genRandomName("CatalogHiveIT_catalog");
  public static final String SCHEMA_PREFIX = "ProxyCatalogHiveIT_schema";
  public static final String TABLE_PREFIX = "ProxyCatalogHiveIT_table";
  private static final String PROVIDER = "hive";
  private static final String EXPECT_USER = "datastrato";
  private static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static GravitinoMetalake metalake;
  private static Catalog catalog;
  private static HiveClientPool hiveClientPool;

  private static String HIVE_METASTORE_URIS;
  private static FileSystem hdfs;
  private static String originHadoopUser;
  private static GravitinoAdminClient anotherClient;
  private static GravitinoAdminClient anotherClientWithUsername;
  private static GravitinoAdminClient anotherClientWithNotExistingName;
  private static Catalog anotherCatalog;
  private static Catalog anotherCatalogWithUsername;
  private static Catalog anotherCatalogWithNotExistingName;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    originHadoopUser = System.getenv(HADOOP_USER_NAME);
    setEnv(HADOOP_USER_NAME, null);

    System.setProperty("user.name", "datastrato");

    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
    registerCustomConfigs(configs);
    super.startIntegrationTest();
    containerSuite.startHiveContainer();
    HIVE_METASTORE_URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    HiveConf hiveConf = new HiveConf();
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, HIVE_METASTORE_URIS);

    // Check if Hive client can connect to Hive metastore
    hiveClientPool = new HiveClientPool(1, hiveConf);

    Configuration conf = new Configuration();
    conf.set(
        "fs.defaultFS",
        String.format(
            "hdfs://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT));
    hdfs = FileSystem.get(conf);
    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);

    String uri = "http://" + jettyServerConfig.getHost() + ":" + jettyServerConfig.getHttpPort();
    System.setProperty("user.name", "test");
    anotherClient = GravitinoAdminClient.builder(uri).withSimpleAuth().build();
    anotherClientWithUsername =
        GravitinoAdminClient.builder(uri).withSimpleAuth(EXPECT_USER).build();
    anotherClientWithNotExistingName =
        GravitinoAdminClient.builder(uri).withSimpleAuth("not-exist").build();
    createMetalake();
    createCatalog();
    loadCatalogWithAnotherClient();
  }

  @AfterAll
  public void stop() {
    setEnv(HADOOP_USER_NAME, originHadoopUser);
    anotherClient.close();
    anotherClientWithUsername.close();
    anotherClientWithNotExistingName.close();

    client = null;
  }

  @Test
  public void testOperateSchema() throws Exception {
    // create schema normally using user datastrato
    String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    String anotherSchemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);

    String comment = "comment";
    createSchema(schemaName, comment);

    Database db = hiveClientPool.run(client -> client.getDatabase(schemaName));
    Assertions.assertEquals(EXPECT_USER, db.getOwnerName());
    Assertions.assertEquals(
        EXPECT_USER, hdfs.getFileStatus(new Path(db.getLocationUri())).getOwner());

    // create schema with exception using the system user
    Map<String, String> properties = Maps.newHashMap();
    properties.put(
        "location",
        String.format(
            "hdfs://%s:%d/user/hive/warehouse/%s.db",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT,
            anotherSchemaName.toLowerCase()));
    SupportsSchemas schemas = anotherCatalog.asSchemas();
    Exception e =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> schemas.createSchema(anotherSchemaName, comment, properties));
    Assertions.assertTrue(e.getMessage().contains("AccessControlException Permission denied"));

    // Test the client using `withSimpleAuth(expectUser)`.
    anotherCatalogWithUsername.asSchemas().createSchema(anotherSchemaName, comment, properties);
    db = hiveClientPool.run(client -> client.getDatabase(schemaName));
    Assertions.assertEquals(EXPECT_USER, db.getOwnerName());
    Assertions.assertEquals(
        EXPECT_USER, hdfs.getFileStatus(new Path(db.getLocationUri())).getOwner());

    // Test the client using `withSimpleAuth(unknownUser)`
    properties.put(
        "location",
        String.format(
            "hdfs://%s:%d/user/hive/warehouse/%s.db",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT,
            "new_schema"));
    e =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                anotherCatalogWithNotExistingName
                    .asSchemas()
                    .createSchema("new_schema", comment, properties));
    Assertions.assertTrue(e.getMessage().contains("AccessControlException Permission denied"));
  }

  @Test
  public void testOperateTable() throws Exception {
    // create table normally using user datastrato
    Column[] columns = createColumns();
    String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    String tableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);
    String anotherTableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);

    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);
    NameIdentifier anotherNameIdentifier = NameIdentifier.of(schemaName, anotherTableName);

    String comment = "comment";
    createSchema(schemaName, comment);

    catalog
        .asTableCatalog()
        .createTable(
            nameIdentifier, columns, comment, ImmutableMap.of(), Partitioning.EMPTY_PARTITIONING);
    Table createdTable = catalog.asTableCatalog().loadTable(nameIdentifier);
    String location = createdTable.properties().get("location");
    Assertions.assertEquals(EXPECT_USER, hdfs.getFileStatus(new Path(location)).getOwner());
    org.apache.hadoop.hive.metastore.api.Table hiveTab =
        hiveClientPool.run(client -> client.getTable(schemaName, tableName));
    Assertions.assertEquals(EXPECT_USER, hiveTab.getOwner());

    // create table with exception with system user
    TableCatalog tableCatalog = anotherCatalog.asTableCatalog();
    ImmutableMap<String, String> of = ImmutableMap.of();
    Exception e =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> {
              tableCatalog.createTable(
                  anotherNameIdentifier, columns, comment, of, Partitioning.EMPTY_PARTITIONING);
            });
    Assertions.assertTrue(e.getMessage().contains("AccessControlException Permission denied"));

    // Test the client using `withSimpleAuth(String)`.
    anotherCatalogWithUsername
        .asTableCatalog()
        .createTable(anotherNameIdentifier, columns, comment, of, Partitioning.EMPTY_PARTITIONING);
    Table anotherCreatedTable =
        anotherCatalogWithUsername.asTableCatalog().loadTable(nameIdentifier);
    String anotherLocation = anotherCreatedTable.properties().get("location");
    Assertions.assertEquals(EXPECT_USER, hdfs.getFileStatus(new Path(anotherLocation)).getOwner());
    org.apache.hadoop.hive.metastore.api.Table anotherHiveTab =
        hiveClientPool.run(client -> client.getTable(schemaName, anotherTableName));
    Assertions.assertEquals(EXPECT_USER, anotherHiveTab.getOwner());

    // Test the client using `withSimpleAuth(not-existing)`
    NameIdentifier anotherIdentWithNotExisting = NameIdentifier.of(schemaName, "new_table");
    e =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> {
              anotherCatalogWithNotExistingName
                  .asTableCatalog()
                  .createTable(
                      anotherIdentWithNotExisting,
                      columns,
                      comment,
                      of,
                      Partitioning.EMPTY_PARTITIONING);
            });
    Assertions.assertTrue(e.getMessage().contains("AccessControlException Permission denied"));
  }

  private static void createSchema(String schemaName, String comment) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    properties.put(
        "location",
        String.format(
            "hdfs://%s:%d/user/hive/warehouse/%s.db",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT,
            schemaName.toLowerCase()));
    catalog.asSchemas().createSchema(schemaName, comment, properties);
  }

  @Test
  public void testOperatePartition() throws Exception {

    // create schema
    String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    String tableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);

    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    String comment = "comment";
    createSchema(schemaName, comment);

    // create a partitioned table
    Column[] columns = createColumns();

    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                "comment",
                createProperties(),
                new Transform[] {
                  Transforms.identity(columns[1].name()), Transforms.identity(columns[2].name())
                });

    // add partition "col2=2023-01-02/col3=gravitino_it_test2" by user datastrato
    String[] field1 = new String[] {"col2"};
    String[] field2 = new String[] {"col3"};
    Literal<?> primaryPartition = Literals.dateLiteral(LocalDate.parse("2023-01-02"));
    Literal<?> secondaryPartition = Literals.stringLiteral("gravitino_it_test2");
    Partition identity =
        Partitions.identity(
            new String[][] {field1, field2},
            new Literal<?>[] {primaryPartition, secondaryPartition});

    Partition partition = table.supportPartitions().addPartition(identity);

    org.apache.hadoop.hive.metastore.api.Partition partitionGot =
        hiveClientPool.run(client -> client.getPartition(schemaName, tableName, partition.name()));

    Assertions.assertEquals(
        EXPECT_USER, hdfs.getFileStatus(new Path(partitionGot.getSd().getLocation())).getOwner());

    Literal<?> anotherSecondaryPartition = Literals.stringLiteral("gravitino_it_test3");
    Partition anotherIdentity =
        Partitions.identity(
            new String[][] {field1, field2},
            new Literal<?>[] {primaryPartition, anotherSecondaryPartition});

    // create partition with exception with system user
    TableCatalog tableCatalog = anotherCatalog.asTableCatalog();
    Exception e =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                tableCatalog
                    .loadTable(nameIdentifier)
                    .supportPartitions()
                    .addPartition(anotherIdentity));
    Assertions.assertTrue(e.getMessage().contains("AccessControlException Permission denied"));

    // Test the client using `withSimpleAuth(String)`.
    Partition anotherPartition =
        anotherCatalogWithUsername
            .asTableCatalog()
            .loadTable(nameIdentifier)
            .supportPartitions()
            .addPartition(anotherIdentity);
    org.apache.hadoop.hive.metastore.api.Partition anotherPartitionGot =
        hiveClientPool.run(
            client -> client.getPartition(schemaName, tableName, anotherPartition.name()));

    Assertions.assertEquals(
        EXPECT_USER,
        hdfs.getFileStatus(new Path(anotherPartitionGot.getSd().getLocation())).getOwner());

    // Test the client using `withSimpleAuth(not-existing)`.
    Literal<?> anotherNewSecondaryPartition = Literals.stringLiteral("gravitino_it_test4");
    Partition anotherNewIdentity =
        Partitions.identity(
            new String[][] {field1, field2},
            new Literal<?>[] {primaryPartition, anotherNewSecondaryPartition});
    e =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                anotherCatalogWithNotExistingName
                    .asTableCatalog()
                    .loadTable(nameIdentifier)
                    .supportPartitions()
                    .addPartition(anotherNewIdentity));
    Assertions.assertTrue(e.getMessage().contains("AccessControlException Permission denied"));
  }

  private Column[] createColumns() {
    Column col1 = Column.of("col1", Types.ByteType.get(), "col_1_comment");
    Column col2 = Column.of("col2", Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of("col3", Types.StringType.get(), "col_3_comment");
    return new Column[] {col1, col2, col3};
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(METALAKE_NAME, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(METALAKE_NAME);
    Assertions.assertEquals(METALAKE_NAME, loadMetalake.name());

    metalake = loadMetalake;
  }

  private Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    return properties;
  }

  private static void createCatalog() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(METASTORE_URIS, HIVE_METASTORE_URIS);
    properties.put(IMPERSONATION_ENABLE, "true");

    metalake.createCatalog(CATALOG_NAME, Catalog.Type.RELATIONAL, PROVIDER, "comment", properties);

    catalog = metalake.loadCatalog(CATALOG_NAME);
  }

  private static void loadCatalogWithAnotherClient() {
    GravitinoMetalake metaLake = anotherClient.loadMetalake(METALAKE_NAME);
    anotherCatalog = metaLake.loadCatalog(CATALOG_NAME);

    anotherCatalogWithUsername =
        anotherClientWithUsername.loadMetalake(METALAKE_NAME).loadCatalog(CATALOG_NAME);

    anotherCatalogWithNotExistingName =
        anotherClientWithNotExistingName.loadMetalake(METALAKE_NAME).loadCatalog(CATALOG_NAME);
  }

  public static void setEnv(String key, String value) {
    try {
      Map<String, String> env = System.getenv();
      Class<?> cl = env.getClass();
      Field field = cl.getDeclaredField("m");
      field.setAccessible(true);
      Map<String, String> writableEnv = (Map<String, String>) field.get(env);
      if (value == null) {
        writableEnv.remove(key);
      } else {
        writableEnv.put(key, value);
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to set environment variable", e);
    }
  }
}
