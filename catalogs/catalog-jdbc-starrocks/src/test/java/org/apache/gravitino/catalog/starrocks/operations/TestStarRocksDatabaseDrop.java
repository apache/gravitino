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
package org.apache.gravitino.catalog.starrocks.operations;

import java.sql.Connection;
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

class TestStarRocksDatabaseDrop {

  @ParameterizedTest
  @CsvSource({"true, false", "false, false", "true, true"})
  void testDropRespectsNonEmptySchemaContract(boolean nonEmpty, boolean cascade) throws Exception {
    DataSource dataSource = Mockito.mock(DataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    Statement statement = Mockito.mock(Statement.class);
    ResultSet tables = Mockito.mock(ResultSet.class);
    Mockito.when(dataSource.getConnection()).thenReturn(connection);
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.executeQuery("SHOW TABLES IN `nonempty_schema`")).thenReturn(tables);
    Mockito.when(tables.next()).thenReturn(nonEmpty);
    StarRocksDatabaseOperations operations = new StarRocksDatabaseOperations();
    operations.initialize(dataSource, new JdbcExceptionConverter(), Collections.emptyMap());

    if (nonEmpty && !cascade) {
      NonEmptySchemaException failure =
          Assertions.assertThrowsExactly(
              NonEmptySchemaException.class, () -> operations.delete("nonempty_schema", false));
      Assertions.assertTrue(failure.getMessage().contains("nonempty_schema"));
      Mockito.verify(statement, Mockito.never()).executeUpdate(Mockito.anyString());
    } else {
      Assertions.assertTrue(operations.delete("nonempty_schema", cascade));
      Mockito.verify(statement)
          .executeUpdate("DROP DATABASE `nonempty_schema`" + (cascade ? " FORCE" : ""));
    }
    if (cascade) {
      Mockito.verify(statement, Mockito.never()).executeQuery(Mockito.anyString());
    } else {
      Mockito.verify(tables).close();
    }
    int connections = !nonEmpty && !cascade ? 2 : 1;
    Mockito.verify(statement, Mockito.times(connections)).close();
    Mockito.verify(connection, Mockito.times(connections)).close();
  }
}
