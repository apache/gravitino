/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.hive;

import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.IMPERSONATION_ENABLE;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;
import static com.datastrato.gravitino.server.GravitinoServer.WEBSERVER_CONF_PREFIX;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.auth.AuthenticatorType;
import com.datastrato.gravitino.catalog.hive.HiveClientPool;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.dto.rel.partitioning.Partitioning;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.partitions.Partitions;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ProxyCatalogHiveIT extends AbstractIT {

  public static final String METALAKE_NAME =
      GravitinoITUtils.genRandomName("ProxyCatalogHiveIT_metalake");
  public static final String CATALOG_NAME = GravitinoITUtils.genRandomName("CatalogHiveIT_catalog");
  public static final String SCHEMA_PREFIX = "ProxyCatalogHiveIT_schema";
  public static final String TABLE_PREFIX = "ProxyCatalogHiveIT_table";
  private static final String PROVIDER = "hive";
  private static final String EXPECT_USER = "datastrato";
  private static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static GravitinoMetaLake metalake;
  private static Catalog catalog;
  private static HiveClientPool hiveClientPool;

  private static String HIVE_METASTORE_URIS;
  private static FileSystem hdfs;
  private static String originHadoopUser;
  private static GravitinoClient anotherClient;
  private static Catalog anotherCatalog;

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    originHadoopUser = System.getenv(HADOOP_USER_NAME);
    setEnv(HADOOP_USER_NAME, null);

    System.setProperty("user.name", "datastrato");

    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATOR.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
    registerCustomConfigs(configs);
    AbstractIT.startIntegrationTest();
    containerSuite.startHiveContainer();
    HIVE_METASTORE_URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    HiveConf hiveConf = new HiveConf();
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, HIVE_METASTORE_URIS);

    // Check if hive client can connect to hive metastore
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
    anotherClient = GravitinoClient.builder(uri).withSimpleAuth().build();
    createMetalake();
    createCatalog();
    loadCatalogWithAnotherClient();
  }

  @AfterAll
  public static void stop() throws Exception {
    setEnv(HADOOP_USER_NAME, originHadoopUser);
    anotherClient.close();
  }

  @Test
  public void testOperateSchema() throws Exception {
    // create schema normally using user datastrato
    String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    String anotherSchemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);

    NameIdentifier ident = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName);
    NameIdentifier anotherIdent = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, anotherSchemaName);

    String comment = "comment";
    createSchema(schemaName, ident, comment);

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
            RuntimeException.class, () -> schemas.createSchema(anotherIdent, comment, properties));
    Assertions.assertTrue(e.getMessage().contains("AccessControlException Permission denied"));
  }

  @Test
  public void testOperateTable() throws Exception {
    // create table normally using user datastrato
    Column[] columns = createColumns();
    String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    String tableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);
    String anotherTableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);

    NameIdentifier ident = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName);
    NameIdentifier nameIdentifier =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, tableName);
    NameIdentifier anotherNameIdentifier =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, anotherTableName);

    String comment = "comment";
    createSchema(schemaName, ident, comment);

    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                comment,
                ImmutableMap.of(),
                Partitioning.EMPTY_PARTITIONING);
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
  }

  private static void createSchema(String schemaName, NameIdentifier ident, String comment) {
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
    catalog.asSchemas().createSchema(ident, comment, properties);
  }

  @Test
  public void testOperatePartition() throws Exception {

    // create schema
    String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    String tableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);

    NameIdentifier ident = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName);
    NameIdentifier nameIdentifier =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, tableName);

    String comment = "comment";
    createSchema(schemaName, ident, comment);

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
  }

  private Column[] createColumns() {
    Column col1 = Column.of("col1", Types.ByteType.get(), "col_1_comment");
    Column col2 = Column.of("col2", Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of("col3", Types.StringType.get(), "col_3_comment");
    return new Column[] {col1, col2, col3};
  }

  private static void createMetalake() {
    GravitinoMetaLake[] gravitinoMetaLakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetaLakes.length);

    GravitinoMetaLake createdMetalake =
        client.createMetalake(NameIdentifier.of(METALAKE_NAME), "comment", Collections.emptyMap());
    GravitinoMetaLake loadMetalake = client.loadMetalake(NameIdentifier.of(METALAKE_NAME));
    Assertions.assertEquals(createdMetalake, loadMetalake);

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

    metalake.createCatalog(
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME),
        Catalog.Type.RELATIONAL,
        PROVIDER,
        "comment",
        properties);

    catalog = metalake.loadCatalog(NameIdentifier.of(METALAKE_NAME, CATALOG_NAME));
  }

  private static void loadCatalogWithAnotherClient() {
    GravitinoMetaLake metaLake = anotherClient.loadMetalake(NameIdentifier.of(METALAKE_NAME));
    anotherCatalog = metaLake.loadCatalog(NameIdentifier.of(METALAKE_NAME, CATALOG_NAME));
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
