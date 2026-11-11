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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

class TestClickHouseDatabaseDrop {

  @ParameterizedTest
  @CsvSource({"true, false", "false, false", "true, true"})
  void testDropRespectsNonEmptySchemaContract(boolean nonEmpty, boolean cascade) throws Exception {
    DataSource dataSource = Mockito.mock(DataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement check = Mockito.mock(PreparedStatement.class);
    PreparedStatement comment = Mockito.mock(PreparedStatement.class);
    Statement statement = Mockito.mock(Statement.class);
    ResultSet tables = Mockito.mock(ResultSet.class);
    ResultSet comments = Mockito.mock(ResultSet.class);
    String checkSql = "SELECT 1 FROM system.tables WHERE database = ? LIMIT 1";
    String commentSql = "SELECT comment FROM system.databases WHERE name = ?";
    Mockito.when(dataSource.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(checkSql)).thenReturn(check);
    Mockito.when(connection.prepareStatement(commentSql)).thenReturn(comment);
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(check.executeQuery()).thenReturn(tables);
    Mockito.when(comment.executeQuery()).thenReturn(comments);
    Mockito.when(tables.next()).thenReturn(nonEmpty);
    ClickHouseDatabaseOperations operations = new ClickHouseDatabaseOperations();
    operations.initialize(dataSource, new JdbcExceptionConverter(), Collections.emptyMap());

    if (nonEmpty && !cascade) {
      NonEmptySchemaException failure =
          Assertions.assertThrowsExactly(
              NonEmptySchemaException.class, () -> operations.delete("nonempty_schema", false));
      Assertions.assertTrue(failure.getMessage().contains("nonempty_schema"));
      Mockito.verify(statement, Mockito.never()).executeUpdate(Mockito.anyString());
      Mockito.verify(connection, Mockito.never()).prepareStatement(commentSql);
    } else {
      Assertions.assertTrue(operations.delete("nonempty_schema", cascade));
      Mockito.verify(statement).executeUpdate("DROP DATABASE `nonempty_schema`");
    }
    if (cascade) {
      Mockito.verify(connection, Mockito.never()).prepareStatement(checkSql);
    } else {
      Mockito.verify(check).setString(1, "nonempty_schema");
      Mockito.verify(tables).close();
      Mockito.verify(check).close();
    }
    Mockito.verify(connection).close();
  }
}
