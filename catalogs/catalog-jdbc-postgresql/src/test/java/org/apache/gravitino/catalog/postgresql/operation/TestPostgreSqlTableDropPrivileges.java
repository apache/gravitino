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
package org.apache.gravitino.catalog.postgresql.operation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlColumnDefaultValueConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlExceptionConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A drop the catalog user may not run must be refused, not reported as a missing table. */
public class TestPostgreSqlTableDropPrivileges {
  private static final String QUALIFIED_DROP = "DROP TABLE \"restricted\".\"t\"\"x\"";

  private Connection connection;
  private Statement statement;
  private PostgreSqlTableOperations operations;

  @BeforeEach
  void setUp() throws Exception {
    DataSource source = mock(DataSource.class);
    connection = mock(Connection.class);
    statement = mock(Statement.class);
    when(source.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    operations = new PostgreSqlTableOperations();
    operations.initialize(
        source,
        new PostgreSqlExceptionConverter(),
        new PostgreSqlTypeConverter(),
        new PostgreSqlColumnDefaultValueConverter(),
        Map.of("jdbc-database", "database"));
  }

  @Test
  void testDropUsesSchemaQualifiedName() throws Exception {
    Assertions.assertTrue(operations.drop("restricted", "t\"x"));
    verify(statement).executeUpdate(QUALIFIED_DROP);
  }

  @Test
  void testSchemaWithoutUsageIsForbidden() throws Exception {
    when(statement.executeUpdate(QUALIFIED_DROP))
        .thenThrow(new SQLException("permission denied for schema restricted", "42501"));
    ForbiddenException error =
        Assertions.assertThrows(
            ForbiddenException.class, () -> operations.drop("restricted", "t\"x"));
    Assertions.assertTrue(error.getMessage().contains("permission denied"));
    verify(connection).close();
  }

  @Test
  void testMissingTableStillReportsNotDropped() throws Exception {
    when(statement.executeUpdate(QUALIFIED_DROP))
        .thenThrow(new SQLException("table \"t\" does not exist", "42P01"));
    Assertions.assertFalse(operations.drop("restricted", "t\"x"));
  }

  @Test
  void testMissingSchemaStillReportsNotDropped() throws Exception {
    when(statement.executeUpdate(QUALIFIED_DROP))
        .thenThrow(new SQLException("schema \"restricted\" does not exist", "3F000"));
    Assertions.assertFalse(operations.drop("restricted", "t\"x"));
  }
}
