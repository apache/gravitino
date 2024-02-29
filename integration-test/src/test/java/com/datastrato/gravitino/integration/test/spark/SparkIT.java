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
    sql("use " + hiveCatalogName);
  }

  @Test
  void testLoadCatalogs() {
    Set<String> catalogs = getCatalogs();
    Assertions.assertTrue(catalogs.contains(hiveCatalogName));
  }

  @Test
  void testCreateAndLoadSchema() {
    String testDatabaseName = "t_create1";
    sql("create database " + testDatabaseName);
    Map<String, String> databaseMeta = getDatabaseMetadata(testDatabaseName);
    Assertions.assertFalse(databaseMeta.containsKey("Comment"));
    Assertions.assertTrue(databaseMeta.containsKey("Location"));
    Assertions.assertEquals("datastrato", databaseMeta.get("Owner"));
    String properties = databaseMeta.get("Properties");
    Assertions.assertTrue(StringUtils.isBlank(properties));

    testDatabaseName = "t_create2";
    sql(
        String.format(
            "CREATE DATABASE %s COMMENT 'comment' LOCATION '/user'\n"
                + " WITH DBPROPERTIES (ID=001);",
            testDatabaseName));
    databaseMeta = getDatabaseMetadata(testDatabaseName);
    String comment = databaseMeta.get("Comment");
    Assertions.assertEquals("comment", comment);
    Assertions.assertEquals("datastrato", databaseMeta.get("Owner"));
    // underlying catalog may change /user to file:/user
    Assertions.assertTrue(databaseMeta.get("Location").contains("/user"));
    properties = databaseMeta.get("Properties");
    Assertions.assertEquals("((ID,001))", properties);
  }

  @Test
  void testDropSchema() {
    String testDatabaseName = "t_drop";
    Set<String> databases = getDatabases();
    Assertions.assertFalse(databases.contains(testDatabaseName));

    sql("create database " + testDatabaseName);
    databases = getDatabases();
    Assertions.assertTrue(databases.contains(testDatabaseName));

    sql("drop database " + testDatabaseName);
    databases = getDatabases();
    Assertions.assertFalse(databases.contains(testDatabaseName));

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class, () -> sql("drop database notExists"));
  }
}
