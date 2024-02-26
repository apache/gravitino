/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc.mysql;

import static com.datastrato.gravitino.catalog.mysql.operation.MysqlDatabaseOperations.SYS_MYSQL_DATABASE_NAMES;

import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public class TestMysqlDatabaseOperations extends TestMysqlAbstractIT {

  @Test
  void testBaseOperationDatabase() {
    String databaseName = GravitinoITUtils.genRandomName("ct_db");
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
