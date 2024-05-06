/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.integration.test;

import static com.datastrato.gravitino.catalog.mysql.operation.MysqlDatabaseOperations.SYS_MYSQL_DATABASE_NAMES;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.utils.RandomNameUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
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

  @Test
  void testDropTableWithSpecificName() {
    String databaseName = RandomNameUtils.genRandomName("ct_db") + "-abc-" + "end";
    Map<String, String> properties = new HashMap<>();
    DATABASE_OPERATIONS.create(databaseName, null, properties);
    DATABASE_OPERATIONS.delete(databaseName, false);

    databaseName = RandomNameUtils.genRandomName("ct_db") + "--------end";
    DATABASE_OPERATIONS.create(databaseName, null, properties);

    String tableName = RandomStringUtils.randomAlphabetic(16) + "_op_table";
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.VarCharType.of(100))
            .withComment("test_comment")
            .withNullable(true)
            .build());

    TABLE_OPERATIONS.create(
        databaseName,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        new Transform[0],
        Distributions.NONE,
        SortOrders.EMPTY_SORT_ORDERS,
        new Index[0]);
    DATABASE_OPERATIONS.delete(databaseName, true);
  }
}
