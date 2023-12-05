/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc.mysql;

import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public class TestMysqlDatabaseOperations extends TestMysqlAbstractIT {

  @Test
  public void testBaseOperationDatabase() {
    String databaseName = RandomUtils.nextInt(10000) + "_ct_db";
    Map<String, String> properties = new HashMap<>();
    // TODO #804 Properties will be unified in the future.
    //    properties.put("CHARACTER SET", "utf8mb3");
    //    properties.put("COLLATE", "utf8mb3_general_ci");
    // Mysql database creation does not support incoming comments.
    String comment = null;
    // create database.
    MYSQL_DATABASE_OPERATIONS.create(databaseName, comment, properties);

    List<String> databases = MYSQL_DATABASE_OPERATIONS.listDatabases();
    Assertions.assertTrue(databases.contains(databaseName));
    // load database.
    JdbcSchema load = MYSQL_DATABASE_OPERATIONS.load(databaseName);

    Assertions.assertEquals(databaseName, load.name());
    Assertions.assertEquals(comment, load.comment());

    //    Assertions.assertTrue(properties.values().containsAll(load.properties().values()));

    // delete database.
    UnsupportedOperationException unsupportedOperationException =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> MYSQL_DATABASE_OPERATIONS.delete(databaseName, true));
    Assertions.assertTrue(
        unsupportedOperationException
            .getMessage()
            .contains("MySQL does not support CASCADE option for DROP DATABASE."));

    Assertions.assertDoesNotThrow(() -> MYSQL_DATABASE_OPERATIONS.delete(databaseName, false));

    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> MYSQL_DATABASE_OPERATIONS.load(databaseName));
    databases = MYSQL_DATABASE_OPERATIONS.listDatabases();
    Assertions.assertFalse(databases.contains(databaseName));
  }
}
