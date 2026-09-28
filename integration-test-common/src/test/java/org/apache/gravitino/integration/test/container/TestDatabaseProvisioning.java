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
package org.apache.gravitino.integration.test.container;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DatabaseProvisioning}'s pure validation/dispatch logic, plus the PostgreSQL
 * exists-check/duplicate-tolerant create sequence and the MySQL statement it issues, both exercised
 * here against a mocked {@link Statement} rather than a live server. The connect-then-delegate
 * paths ({@code createMySQLDatabaseIfAbsent}/{@code createPostgreSQLDatabaseIfAbsent}) still need a
 * live server and are exercised indirectly by the docker-tagged integration tests instead.
 */
public class TestDatabaseProvisioning {

  @Test
  public void testIsValidDatabaseNameAcceptsAllowedCharacters() {
    assertTrue(DatabaseProvisioning.isValidDatabaseName("gravitino_core_w1"));
    assertTrue(DatabaseProvisioning.isValidDatabaseName("a"));
    assertTrue(DatabaseProvisioning.isValidDatabaseName("A1_$"));
  }

  @Test
  public void testIsValidDatabaseNameRejectsNullOrEmpty() {
    assertFalse(DatabaseProvisioning.isValidDatabaseName(null));
    assertFalse(DatabaseProvisioning.isValidDatabaseName(""));
  }

  @Test
  public void testIsValidDatabaseNameRejectsDisallowedCharacters() {
    assertFalse(DatabaseProvisioning.isValidDatabaseName("bad-name"));
    assertFalse(DatabaseProvisioning.isValidDatabaseName("bad name"));
    assertFalse(DatabaseProvisioning.isValidDatabaseName("bad;name"));
    assertFalse(DatabaseProvisioning.isValidDatabaseName("bad'name"));
  }

  @Test
  public void testIsValidDatabaseNameRejectsNamesOverSixtyThreeCharacters() {
    String sixtyThree = "a".repeat(63);
    String sixtyFour = "a".repeat(64);
    assertTrue(DatabaseProvisioning.isValidDatabaseName(sixtyThree));
    assertFalse(DatabaseProvisioning.isValidDatabaseName(sixtyFour));
  }

  @Test
  public void testCreateDatabaseIfAbsentRejectsInvalidName() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DatabaseProvisioning.createDatabaseIfAbsent(
                    "jdbc:mysql://127.0.0.1:3306", "root", "root", "bad-name"));
    assertTrue(e.getMessage().contains("Invalid database name"));
  }

  @Test
  public void testCreateDatabaseIfAbsentRejectsUnsupportedJdbcUrl() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DatabaseProvisioning.createDatabaseIfAbsent(
                    "jdbc:h2:mem:test", "root", "root", "valid_name"));
    assertTrue(e.getMessage().contains("Unsupported JDBC URL"));
  }

  @Test
  public void testExecuteCreateMySQLDatabaseIssuesCreateIfNotExists() throws SQLException {
    Statement statement = mock(Statement.class);
    DatabaseProvisioning.executeCreateMySQLDatabase(statement, "gravitino_core_1");
    verify(statement).execute("CREATE DATABASE IF NOT EXISTS `gravitino_core_1`");
  }

  @Test
  public void testPostgreSQLDatabaseExistsTrueWhenQueryReturnsARow() throws SQLException {
    Statement statement = mock(Statement.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.executeQuery(contains("gravitino_core_1"))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);

    assertTrue(DatabaseProvisioning.postgreSQLDatabaseExists(statement, "gravitino_core_1"));
  }

  @Test
  public void testPostgreSQLDatabaseExistsFalseWhenQueryReturnsNoRow() throws SQLException {
    Statement statement = mock(Statement.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.executeQuery(anyString())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    assertFalse(DatabaseProvisioning.postgreSQLDatabaseExists(statement, "gravitino_core_1"));
  }

  @Test
  public void testExecuteCreatePostgreSQLDatabaseSkipsCreateWhenAlreadyExists()
      throws SQLException {
    Statement statement = mock(Statement.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.executeQuery(anyString())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true); // exists-check finds a row

    DatabaseProvisioning.executeCreatePostgreSQLDatabase(statement, "gravitino_core_1");

    verify(statement, never()).execute(anyString());
  }

  @Test
  public void testExecuteCreatePostgreSQLDatabaseIssuesCreateWhenAbsent() throws SQLException {
    Statement statement = mock(Statement.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.executeQuery(anyString())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false); // exists-check finds no row

    DatabaseProvisioning.executeCreatePostgreSQLDatabase(statement, "gravitino_core_1");

    verify(statement).execute("CREATE DATABASE \"gravitino_core_1\"");
  }

  @Test
  public void testExecuteCreatePostgreSQLDatabaseTreatsDuplicateSqlStateAsSuccess()
      throws SQLException {
    Statement statement = mock(Statement.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.executeQuery(anyString())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);
    // 42P04 = PostgreSQL's duplicate_database SQLSTATE: another caller won the race between this
    // method's own exists-check and its CREATE DATABASE statement.
    when(statement.execute(anyString()))
        .thenThrow(new SQLException("database already exists", "42P04"));

    // Must not throw.
    DatabaseProvisioning.executeCreatePostgreSQLDatabase(statement, "gravitino_core_1");
  }

  @Test
  public void testExecuteCreatePostgreSQLDatabaseRethrowsNonDuplicateSqlState()
      throws SQLException {
    Statement statement = mock(Statement.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.executeQuery(anyString())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);
    SQLException permissionDenied = new SQLException("permission denied", "42501");
    when(statement.execute(anyString())).thenThrow(permissionDenied);

    SQLException thrown =
        assertThrows(
            SQLException.class,
            () ->
                DatabaseProvisioning.executeCreatePostgreSQLDatabase(
                    statement, "gravitino_core_1"));
    assertTrue(thrown == permissionDenied);
  }
}
