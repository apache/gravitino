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
package org.apache.gravitino.catalog.clickhouse.operations;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.jdbc.utils.ConnectionCountingDataSource;
import org.apache.gravitino.rel.TableChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests that altering a table never holds one pooled connection while borrowing another. */
public class TestClickHouseTableOperationsConnectionUsage {

  private static final String ALTER_SQL = "ALTER TABLE `orders` MODIFY COMMENT 'comment'";

  @Test
  public void testAlterTableGeneratesSqlWithoutHoldingAConnection() {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    List<Integer> borrowedDuringGeneration = new ArrayList<>();
    ClickHouseTableOperations operations =
        new ClickHouseTableOperations() {
          {
            dataSource = connections.dataSource();
            exceptionMapper = new ClickHouseExceptionConverter();
          }

          @Override
          protected String generateAlterTableSql(
              String databaseName, String tableName, TableChange... changes) {
            borrowedDuringGeneration.add(connections.borrowed());
            // Like loading the original table, borrow a connection while generating.
            try (Connection ignored = dataSource.getConnection()) {
              return ALTER_SQL;
            } catch (SQLException e) {
              throw new IllegalStateException(e);
            }
          }
        };

    operations.alterTable("db", "orders", TableChange.updateComment("comment"));

    Assertions.assertEquals(List.of(0), borrowedDuringGeneration);
    Assertions.assertEquals(1, connections.peakBorrowed());
    Assertions.assertEquals(List.of(ALTER_SQL), connections.executedSql());
    Assertions.assertEquals(0, connections.borrowed());
  }
}
