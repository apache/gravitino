/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark;

import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.platform.commons.util.StringUtils;

@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class SparkIT extends SparkEnvIT {

  @BeforeEach
  void init() {
    sql("USE " + hiveCatalogName);
  }

  @Test
  void testLoadCatalogs() {
    Set<String> catalogs = getCatalogs();
    Assertions.assertTrue(catalogs.contains(hiveCatalogName));
  }

  @Test
  void testCreateAndLoadSchema() {
    String testDatabaseName = "t_create1";
    sql("CREATE DATABASE " + testDatabaseName);
    Map<String, String> databaseMeta = getDatabaseMetadata(testDatabaseName);
    Assertions.assertFalse(databaseMeta.containsKey("Comment"));
    Assertions.assertTrue(databaseMeta.containsKey("Location"));
    Assertions.assertEquals("datastrato", databaseMeta.get("Owner"));
    String properties = databaseMeta.get("Properties");
    Assertions.assertTrue(StringUtils.isBlank(properties));

    testDatabaseName = "t_create2";
    String testDatabaseLocation = "/tmp/" + testDatabaseName;
    sql(
        String.format(
            "CREATE DATABASE %s COMMENT 'comment' LOCATION '%s'\n" + " WITH DBPROPERTIES (ID=001);",
            testDatabaseName, testDatabaseLocation));
    databaseMeta = getDatabaseMetadata(testDatabaseName);
    String comment = databaseMeta.get("Comment");
    Assertions.assertEquals("comment", comment);
    Assertions.assertEquals("datastrato", databaseMeta.get("Owner"));
    // underlying catalog may change /tmp/t_create2 to file:/tmp/t_create2
    Assertions.assertTrue(databaseMeta.get("Location").contains(testDatabaseLocation));
    properties = databaseMeta.get("Properties");
    Assertions.assertEquals("((ID,001))", properties);
  }

  @Test
  void testAlterSchema() {
    String testDatabaseName = "t_alter";
    sql("CREATE DATABASE " + testDatabaseName);
    Assertions.assertTrue(
        StringUtils.isBlank(getDatabaseMetadata(testDatabaseName).get("Properties")));

    sql(String.format("ALTER DATABASE %s SET DBPROPERTIES ('ID'='001')", testDatabaseName));
    Assertions.assertEquals("((ID,001))", getDatabaseMetadata(testDatabaseName).get("Properties"));

    // Hive metastore doesn't support alter database location, therefore this test method
    // doesn't verify ALTER DATABASE database_name SET LOCATION 'new_location'.

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class,
        () -> sql("ALTER DATABASE notExists SET DBPROPERTIES ('ID'='001')"));
  }

  @Test
  void testDropSchema() {
    String testDatabaseName = "t_drop";
    Set<String> databases = getDatabases();
    Assertions.assertFalse(databases.contains(testDatabaseName));

    sql("CREATE DATABASE " + testDatabaseName);
    databases = getDatabases();
    Assertions.assertTrue(databases.contains(testDatabaseName));

    sql("DROP DATABASE " + testDatabaseName);
    databases = getDatabases();
    Assertions.assertFalse(databases.contains(testDatabaseName));

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class, () -> sql("DROP DATABASE notExists"));
  }
}
