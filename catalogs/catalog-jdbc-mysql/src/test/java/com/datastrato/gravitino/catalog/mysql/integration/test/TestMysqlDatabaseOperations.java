/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.integration.test;

import static com.datastrato.gravitino.catalog.mysql.operation.MysqlDatabaseOperations.SYS_MYSQL_DATABASE_NAMES;

import com.datastrato.gravitino.utils.RandomNameUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public class TestMysqlDatabaseOperations extends TestMysqlAbstractIT {

  @Test
  public void testBaseOperationDatabase() {
    String databaseName = RandomNameUtils.genRandomName("ct_db");
    Map<String, String> properties = new HashMap<>();
    // Mysql database creation does not support incoming comments.
    String comment = null;
    List<String> databases = DATABASE_OPERATIONS.listDatabases();
    SYS_MYSQL_DATABASE_NAMES.forEach(
        sysMysqlDatabaseName -> Assertions.assertFalse(databases.contains(sysMysqlDatabaseName)));
    testBaseOperation(databaseName, properties, comment);
    testDropDatabase(databaseName);
  }
}
