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
package org.apache.gravitino.catalog.doris.operation;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.doris.converter.DorisColumnDefaultValueConverter;
import org.apache.gravitino.catalog.doris.converter.DorisExceptionConverter;
import org.apache.gravitino.catalog.doris.converter.DorisTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class TestDorisTableCreation {
  private static final String CREATE_SQL = "CREATE TABLE `t` (id INT)";
  private static final String COMMENT =
      "a real comment (From Gravitino, DO NOT EDIT: gravitino.v1.uid123)";

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"OLAP", "different comment"})
  void testRepairDiscardedComment(String storedComment) throws Exception {
    CreateFixture fixture = new CreateFixture(COMMENT, storedComment);

    fixture.create();

    verify(fixture.createStatement).executeUpdate(CREATE_SQL);
    verify(fixture.alterStatement)
        .executeUpdate("ALTER TABLE `t` MODIFY COMMENT \"" + COMMENT + "\"");
    verify(fixture.commentStatement).setString(1, "db");
    verify(fixture.commentStatement).setString(2, "t");
    verify(fixture.commentResult).close();
    verify(fixture.commentStatement).close();
    verify(fixture.connection, times(2)).close();
  }

  @Test
  void testRepairEscapesQuotesAndBackslashes() throws Exception {
    String comment =
        "owner's \"comment\" C:\\tmp (From Gravitino, DO NOT EDIT: gravitino.v1.uid123)";
    CreateFixture fixture = new CreateFixture(comment, "OLAP");

    fixture.create();

    verify(fixture.alterStatement)
        .executeUpdate(
            "ALTER TABLE `t` MODIFY COMMENT \"owner's \"\"comment\"\" C:\\\\tmp "
                + "(From Gravitino, DO NOT EDIT: gravitino.v1.uid123)\"");
  }

  @Test
  void testMissingTableIsNotTreatedAsEmptyComment() throws Exception {
    CreateFixture fixture = new CreateFixture(COMMENT, "");
    when(fixture.commentResult.next()).thenReturn(false);

    GravitinoRuntimeException error =
        assertThrows(GravitinoRuntimeException.class, fixture::create);

    assertInstanceOf(NoSuchTableException.class, error.getCause());
    assertTrue(error.getCause().getMessage().contains("Table db.t does not exist in Doris"));
    assertTrue(error.getMessage().contains("Table db.t was created in Doris"));
    assertTrue(error.getMessage().contains("Drop the created table in Doris before retrying"));
    verify(fixture.alterStatement, never()).executeUpdate(anyString());
    verify(fixture.commentResult).close();
    verify(fixture.commentStatement).close();
    verify(fixture.connection, times(2)).close();
  }

  @Test
  void testPreservedCommentNeedsNoAlter() throws Exception {
    CreateFixture fixture = new CreateFixture(COMMENT, COMMENT);

    fixture.create();

    verify(fixture.alterStatement, never()).executeUpdate(anyString());
  }

  @ParameterizedTest
  @NullAndEmptySource
  void testAbsentCommentNeedsNoRepair(String comment) throws Exception {
    CreateFixture fixture = new CreateFixture(comment, "OLAP");

    fixture.create();

    verify(fixture.connection, never()).prepareStatement(anyString());
    verify(fixture.alterStatement, never()).executeUpdate(anyString());
    verify(fixture.connection).close();
  }

  @Test
  void testFailedCreateDoesNotModifyExistingTable() throws Exception {
    CreateFixture fixture = new CreateFixture(COMMENT, "existing comment");
    when(fixture.createStatement.executeUpdate(CREATE_SQL))
        .thenThrow(new SQLException("Table already exists", "42S01", 1050));

    assertThrows(TableAlreadyExistsException.class, fixture::create);

    verify(fixture.connection, never()).prepareStatement(anyString());
    verify(fixture.alterStatement, never()).executeUpdate(anyString());
    verify(fixture.connection).close();
  }

  @Test
  void testCommentLookupFailureIsReported() throws Exception {
    CreateFixture fixture = new CreateFixture(COMMENT, "OLAP");
    SQLException failure = new SQLException("comment lookup failed");
    when(fixture.commentStatement.executeQuery()).thenThrow(failure);

    GravitinoRuntimeException error =
        assertThrows(GravitinoRuntimeException.class, fixture::create);

    assertSame(failure, error.getCause());
    assertTrue(error.getMessage().contains("Table db.t was created in Doris"));
    assertTrue(error.getMessage().contains("may be missing its Gravitino identifier"));
    assertTrue(error.getMessage().contains("Drop the created table in Doris before retrying"));
    verify(fixture.alterStatement, never()).executeUpdate(anyString());
    verify(fixture.commentStatement).close();
    verify(fixture.connection, times(2)).close();
  }

  @Test
  void testCommentRepairFailureIsReported() throws Exception {
    CreateFixture fixture = new CreateFixture(COMMENT, "OLAP");
    SQLException failure = new SQLException("comment repair failed");
    when(fixture.alterStatement.executeUpdate(anyString())).thenThrow(failure);

    GravitinoRuntimeException error =
        assertThrows(GravitinoRuntimeException.class, fixture::create);

    assertSame(failure, error.getCause());
    assertTrue(error.getMessage().contains("Table db.t was created in Doris"));
    assertTrue(error.getMessage().contains("may be missing its Gravitino identifier"));
    assertTrue(error.getMessage().contains("Drop the created table in Doris before retrying"));
    verify(fixture.alterStatement).close();
    verify(fixture.connection, times(2)).close();
  }

  private static class CreateFixture {
    private final Connection connection = mock(Connection.class);
    private final Statement createStatement = mock(Statement.class);
    private final Statement alterStatement = mock(Statement.class);
    private final PreparedStatement commentStatement = mock(PreparedStatement.class);
    private final ResultSet commentResult = mock(ResultSet.class);
    private final DorisTableOperations operations = spy(new DorisTableOperations());
    private final JdbcColumn[] columns = new JdbcColumn[0];
    private final Index[] indexes = new Index[0];
    @Nullable private final String comment;

    private CreateFixture(@Nullable String comment, @Nullable String storedComment)
        throws Exception {
      this.comment = comment;
      DataSource dataSource = mock(DataSource.class);
      when(dataSource.getConnection()).thenReturn(connection);
      when(connection.createStatement()).thenReturn(createStatement, alterStatement);
      when(connection.prepareStatement(startsWith("SELECT TABLE_COMMENT")))
          .thenReturn(commentStatement);
      when(commentStatement.executeQuery()).thenReturn(commentResult);
      when(commentResult.next()).thenReturn(true, false);
      when(commentResult.getString("TABLE_COMMENT")).thenReturn(storedComment);
      operations.initialize(
          dataSource,
          new DorisExceptionConverter(),
          new DorisTypeConverter(),
          new DorisColumnDefaultValueConverter(),
          Collections.emptyMap());
      doReturn(CREATE_SQL)
          .when(operations)
          .generateCreateTableSql(
              "t",
              columns,
              comment,
              Collections.emptyMap(),
              Transforms.EMPTY_TRANSFORM,
              Distributions.NONE,
              indexes);
    }

    private void create() {
      operations.create(
          "db",
          "t",
          columns,
          comment,
          Collections.emptyMap(),
          Transforms.EMPTY_TRANSFORM,
          Distributions.NONE,
          indexes);
    }
  }
}
