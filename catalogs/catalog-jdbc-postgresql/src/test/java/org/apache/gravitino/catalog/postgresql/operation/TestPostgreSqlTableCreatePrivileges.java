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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlColumnDefaultValueConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlExceptionConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** A hidden search-path entry must not make an existing inaccessible schema look absent. */
public class TestPostgreSqlTableCreatePrivileges {
  private DataSource source;
  private Connection createConnection;
  private Connection probeConnection;
  private Statement createStatement;
  private PreparedStatement probeStatement;
  private ResultSet rows;
  private PostgreSqlTableOperations operations;

  @BeforeEach
  void setUp() throws Exception {
    source = mock(DataSource.class);
    createConnection = mock(Connection.class);
    probeConnection = mock(Connection.class);
    createStatement = mock(Statement.class);
    probeStatement = mock(PreparedStatement.class);
    rows = mock(ResultSet.class);
    when(source.getConnection()).thenReturn(createConnection, probeConnection);
    when(createConnection.createStatement()).thenReturn(createStatement);
    when(createStatement.executeUpdate(anyString()))
        .thenThrow(new SQLException("no schema has been selected to create in", "3F000"));
    when(probeConnection.prepareStatement(anyString())).thenReturn(probeStatement);
    when(probeStatement.executeQuery()).thenReturn(rows);
    operations = new PostgreSqlTableOperations();
    operations.initialize(
        source,
        new PostgreSqlExceptionConverter(),
        new PostgreSqlTypeConverter(),
        new PostgreSqlColumnDefaultValueConverter(),
        Map.of("jdbc-database", "database"));
  }

  @Test
  void testExistingSchemaWithoutUsageIsForbidden() throws Exception {
    when(rows.next()).thenReturn(true);
    when(rows.getBoolean(1)).thenReturn(false);
    ForbiddenException error = Assertions.assertThrows(ForbiddenException.class, this::create);
    Assertions.assertTrue(error.getMessage().contains("restricted"));
    Assertions.assertTrue(error.getMessage().contains("USAGE"));
    verify(probeStatement).setString(1, "restricted");
    verify(createConnection).close();
    verify(probeConnection).close();
    verify(probeStatement).close();
    verify(rows).close();
  }

  @Test
  void testMissingSchemaRemainsMissing() throws Exception {
    when(rows.next()).thenReturn(false);
    Assertions.assertThrows(NoSuchSchemaException.class, this::create);
    verify(probeConnection).close();
  }

  @Test
  void testExistingUsableSchemaDoesNotInventPermissionFailure() throws Exception {
    when(rows.next()).thenReturn(true);
    when(rows.getBoolean(1)).thenReturn(true);
    Assertions.assertThrows(NoSuchSchemaException.class, this::create);
    verify(probeConnection).close();
  }

  @Test
  void testSuccessfulCreateDoesNotBorrowForPermissionProbe() throws Exception {
    doReturn(0).when(createStatement).executeUpdate(anyString());
    Assertions.assertDoesNotThrow(this::create);
    verify(source, times(1)).getConnection();
    verify(createConnection).close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testProbeFailurePreservesCreateFailure(boolean queryFailure) throws Exception {
    SQLException probeFailure = new SQLException("connection lost", "08006");
    if (queryFailure) {
      when(probeStatement.executeQuery()).thenThrow(probeFailure);
    } else {
      when(source.getConnection()).thenReturn(createConnection).thenThrow(probeFailure);
    }
    ConnectionFailedException error =
        Assertions.assertThrows(ConnectionFailedException.class, this::create);
    Assertions.assertSame(probeFailure, error.getCause());
    Assertions.assertEquals(1, error.getSuppressed().length);
    NoSuchSchemaException createFailure =
        Assertions.assertInstanceOf(NoSuchSchemaException.class, error.getSuppressed()[0]);
    SQLException createCause =
        Assertions.assertInstanceOf(SQLException.class, createFailure.getCause());
    Assertions.assertEquals("3F000", createCause.getSQLState());
    Assertions.assertEquals("no schema has been selected to create in", createCause.getMessage());
    verify(createConnection).close();
    if (queryFailure) {
      verify(probeConnection).close();
      verify(probeStatement).close();
    }
  }

  private void create() {
    operations.create(
        "restricted",
        "t",
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("id")
              .withType(Types.IntegerType.get())
              .withNullable(true)
              .build()
        },
        null,
        Map.of(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);
  }
}
