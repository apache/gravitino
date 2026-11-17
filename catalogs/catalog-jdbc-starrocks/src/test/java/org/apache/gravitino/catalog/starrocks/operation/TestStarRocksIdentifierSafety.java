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
package org.apache.gravitino.catalog.starrocks.operation;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTablePartitionOperations;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Verifies rejected identifiers cannot alias existing StarRocks objects. */
@Tag("gravitino-docker-test")
class TestStarRocksIdentifierSafety extends TestStarRocks {
  private String databaseName;

  @BeforeEach
  void createDatabase() throws Exception {
    databaseName = RandomNameUtils.genRandomName("identifier_safety_");
    try (Connection connection = DATA_SOURCE.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + databaseName);
    }
  }

  @AfterEach
  void dropDatabase() throws Exception {
    try (Connection connection = DATA_SOURCE.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS " + databaseName);
    }
  }

  @Test
  void testDropCannotAliasAnotherTable() throws Exception {
    createTable("");
    Assertions.assertEquals(
        Collections.singletonList("victim"), TABLE_OPERATIONS.listTables(databaseName));
    Assertions.assertAll(
        () -> assertRejected(() -> TABLE_OPERATIONS.drop(databaseName, "vic`tim"), "vic`tim"),
        () ->
            Assertions.assertEquals(
                Collections.singletonList("victim"), TABLE_OPERATIONS.listTables(databaseName)));
    Assertions.assertTrue(TABLE_OPERATIONS.drop(databaseName, "victim"));
    Assertions.assertFalse(TABLE_OPERATIONS.drop(databaseName, "victim"));
  }

  @Test
  void testRenameCannotAliasAnotherTable() throws Exception {
    createTable("");
    assertRejected(() -> TABLE_OPERATIONS.rename(databaseName, "vic`tim", "renamed"), "vic`tim");
    assertRejected(() -> TABLE_OPERATIONS.rename(databaseName, "victim", "re`named"), "re`named");
    Assertions.assertEquals(
        Collections.singletonList("victim"), TABLE_OPERATIONS.listTables(databaseName));
  }

  @Test
  void testColumnChangesCannotAliasAnotherColumn() throws Exception {
    createTable("");
    TableChange[] changes = {
      TableChange.renameColumn(new String[] {"name"}, "na`me"),
      TableChange.addColumn(new String[] {"na`me"}, Types.IntegerType.get()),
      TableChange.addColumn(
          new String[] {"added"},
          Types.IntegerType.get(),
          TableChange.ColumnPosition.after("na`me")),
      TableChange.updateColumnPosition(
          new String[] {"name"}, TableChange.ColumnPosition.after("i`d"))
    };
    for (TableChange change : changes) {
      Assertions.assertAll(
          () ->
              assertRejected(
                  () -> TABLE_OPERATIONS.alterTable(databaseName, "victim", change), "`"),
          () ->
              Assertions.assertArrayEquals(
                  new String[] {"id", "name"},
                  Arrays.stream(TABLE_OPERATIONS.load(databaseName, "victim").columns())
                      .map(Column::name)
                      .toArray(String[]::new)));
    }
  }

  @Test
  void testPartitionChangesCannotAliasAnotherPartition() throws Exception {
    createTable("PARTITION BY RANGE(id) (PARTITION p1 VALUES LESS THAN (\"10\")) ");
    JdbcTablePartitionOperations partitions =
        TABLE_OPERATIONS.createJdbcTablePartitionOperations(
            TABLE_OPERATIONS.load(databaseName, "victim"));
    Assertions.assertArrayEquals(new String[] {"p1"}, partitions.listPartitionNames());
    Assertions.assertAll(
        () -> assertRejected(() -> partitions.dropPartition("p`1"), "p`1"),
        () -> Assertions.assertArrayEquals(new String[] {"p1"}, partitions.listPartitionNames()));
    Assertions.assertAll(
        () ->
            assertRejected(
                () ->
                    partitions.addPartition(
                        Partitions.range(
                            "p`2",
                            Literals.integerLiteral(20),
                            Literals.integerLiteral(10),
                            Collections.emptyMap())),
                "p`2"),
        () -> Assertions.assertArrayEquals(new String[] {"p1"}, partitions.listPartitionNames()));
    Assertions.assertTrue(partitions.dropPartition("p1"));
    Assertions.assertArrayEquals(new String[0], partitions.listPartitionNames());
  }

  private void createTable(String partitioning) throws Exception {
    try (Connection connection = DATA_SOURCE.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE "
              + databaseName
              + ".victim (id INT, name INT) "
              + "DUPLICATE KEY(id) "
              + partitioning
              + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\")");
    }
  }

  private static void assertRejected(Executable action, String identifier) {
    IllegalArgumentException error =
        Assertions.assertThrows(IllegalArgumentException.class, action);
    Assertions.assertTrue(
        error.getMessage().contains("must not contain backticks"), error.getMessage());
    Assertions.assertTrue(error.getMessage().contains(identifier), error.getMessage());
  }
}
