/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.hive;

import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.IMPERSONATION_ENABLE;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;
import static com.datastrato.gravitino.server.GravitinoServer.WEBSERVER_CONF_PREFIX;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.auth.AuthenticatorType;
import com.datastrato.gravitino.catalog.hive.HiveClientPool;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.partitions.Partitioning;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.server.auth.OAuthConfig;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.lang.reflect.Field;
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
    configs.put(OAuthConfig.AUTHENTICATOR.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
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
    String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    String anotherSchemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    NameIdentifier ident = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName);
    NameIdentifier anotherIdent = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, anotherSchemaName);
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
    String comment = "comment";

    catalog.asSchemas().createSchema(ident, comment, properties);
    Database db = hiveClientPool.run(client -> client.getDatabase(schemaName));
    Assertions.assertEquals(EXPECT_USER, db.getOwnerName());
    Assertions.assertEquals(
        EXPECT_USER, hdfs.getFileStatus(new Path(db.getLocationUri())).getOwner());
    properties.put(
        "location",
        String.format(
            "hdfs://%s:%d/user/hive/warehouse/%s.db",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT,
            anotherSchemaName.toLowerCase()));
    Assertions.assertThrows(
        RuntimeException.class,
        () -> anotherCatalog.asSchemas().createSchema(anotherIdent, comment, properties));
  }

  @Test
  public void testOperateTable() throws Exception {
    ColumnDTO[] columns = createColumns();
    String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    String tableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);
    String anotherTableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);
    NameIdentifier ident = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName);
    NameIdentifier nameIdentifier =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, tableName);
    NameIdentifier anotherNameIdentifier =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, anotherTableName);
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
    String comment = "comment";
    catalog.asSchemas().createSchema(ident, comment, properties);
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
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            anotherCatalog
                .asTableCatalog()
                .createTable(
                    anotherNameIdentifier,
                    columns,
                    comment,
                    ImmutableMap.of(),
                    Partitioning.EMPTY_PARTITIONING));
  }

  private ColumnDTO[] createColumns() {
    ColumnDTO col1 =
        new ColumnDTO.Builder<>()
            .withName("col1")
            .withDataType(Types.ByteType.get())
            .withComment("col_1_comment")
            .build();
    ColumnDTO col2 =
        new ColumnDTO.Builder<>()
            .withName("col2")
            .withDataType(Types.DateType.get())
            .withComment("col_2_comment")
            .build();
    ColumnDTO col3 =
        new ColumnDTO.Builder<>()
            .withName("col3")
            .withDataType(Types.StringType.get())
            .withComment("col_3_comment")
            .build();
    return new ColumnDTO[] {col1, col2, col3};
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
