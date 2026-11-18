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
package org.apache.gravitino.catalog.oceanbase.operation;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.utils.ConnectionCountingDataSource;
import org.apache.gravitino.rel.TableChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests that altering a table never holds one pooled connection while borrowing another. */
public class TestOceanBaseTableOperationsConnectionUsage {

  @Test
  public void testEachChangeIsGeneratedWithoutHoldingAConnectionAfterThePreviousOneIsApplied() {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    List<Integer> borrowedDuringGeneration = new ArrayList<>();
    List<Integer> executedBeforeGeneration = new ArrayList<>();
    OceanBaseTableOperations operations =
        new OceanBaseTableOperations() {
          {
            dataSource = connections.dataSource();
            exceptionMapper = new JdbcExceptionConverter();
          }

          @Override
          protected String generateAlterTableSql(
              String databaseName, String tableName, TableChange... changes) {
            borrowedDuringGeneration.add(connections.borrowed());
            executedBeforeGeneration.add(connections.executedSql().size());
            if (changes[0] instanceof TableChange.SetProperty) {
              return "";
            }
            // Like loading the original table, borrow a connection while generating.
            try (Connection ignored = dataSource.getConnection()) {
              return "ALTER TABLE `orders` "
                  + ((TableChange.UpdateComment) changes[0]).getNewComment();
            } catch (SQLException e) {
              throw new IllegalStateException(e);
            }
          }
        };

    operations.alterTable(
        "db",
        "orders",
        TableChange.updateComment("first"),
        TableChange.setProperty("key", "value"),
        TableChange.updateComment("second"));

    Assertions.assertEquals(List.of(0, 0, 0), borrowedDuringGeneration);
    // Each change is generated after the previous statement ran, so it sees the updated table.
    Assertions.assertEquals(List.of(0, 1, 1), executedBeforeGeneration);
    Assertions.assertEquals(1, connections.peakBorrowed());
    Assertions.assertEquals(
        List.of("ALTER TABLE `orders` first", "ALTER TABLE `orders` second"),
        connections.executedSql());
    Assertions.assertEquals(0, connections.borrowed());
  }
}
