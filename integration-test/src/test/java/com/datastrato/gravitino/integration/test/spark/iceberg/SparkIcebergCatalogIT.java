/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark.iceberg;

import com.datastrato.gravitino.integration.test.spark.SparkCommonIT;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkIcebergCatalogIT extends SparkCommonIT {

  @Override
  protected String getCatalogName() {
    return "iceberg";
  }

  @Override
  protected String getProvider() {
    return "lakehouse-iceberg";
  }

  @Override
  protected String getUsingClause() {
    return "USING ICEBERG";
  }

  @Test
  void testCreateAndLoadSchema() {
    String testDatabaseName = "t_create1";
    dropDatabaseIfExists(testDatabaseName);
    sql("CREATE DATABASE " + testDatabaseName);
    Map<String, String> databaseMeta = getDatabaseMetadata(testDatabaseName);
    Assertions.assertFalse(databaseMeta.containsKey("Comment"));
    Assertions.assertTrue(databaseMeta.containsKey("Location"));
    Assertions.assertEquals("datastrato", databaseMeta.get("Owner"));
    String properties = databaseMeta.get("Properties");
    Assertions.assertEquals(
        "((hive.metastore.database.owner,datastrato), (hive.metastore.database.owner-type,USER))",
        properties);

    testDatabaseName = "t_create2";
    dropDatabaseIfExists(testDatabaseName);
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
    Assertions.assertEquals(
        "((ID,001), (hive.metastore.database.owner,datastrato), (hive.metastore.database.owner-type,USER))",
        properties);
  }

  @Test
  void testAlterSchema() {
    String testDatabaseName = "t_alter";
    sql("CREATE DATABASE " + testDatabaseName);
    Assertions.assertEquals(
        "((hive.metastore.database.owner,datastrato), (hive.metastore.database.owner-type,USER))",
        getDatabaseMetadata(testDatabaseName).get("Properties"));

    sql(String.format("ALTER DATABASE %s SET DBPROPERTIES ('ID'='001')", testDatabaseName));
    Assertions.assertEquals(
        "((ID,001), (hive.metastore.database.owner,datastrato), (hive.metastore.database.owner-type,USER))",
        getDatabaseMetadata(testDatabaseName).get("Properties"));

    // Hive metastore doesn't support alter database location, therefore this test method
    // doesn't verify ALTER DATABASE database_name SET LOCATION 'new_location'.

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class,
        () -> sql("ALTER DATABASE notExists SET DBPROPERTIES ('ID'='001')"));
  }
}
