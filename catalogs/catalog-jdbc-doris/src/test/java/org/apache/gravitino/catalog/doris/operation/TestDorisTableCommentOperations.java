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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.junit.jupiter.api.Test;

class TestDorisTableCommentOperations {
  @Test
  void testEngineMetadataDoesNotReplaceTableComment() throws Exception {
    Connection connection = mock(Connection.class);
    PreparedStatement commentStatement = mock(PreparedStatement.class);
    ResultSet commentResult = mock(ResultSet.class);
    PreparedStatement statusStatement = mock(PreparedStatement.class);
    ResultSet statusResult = mock(ResultSet.class);
    when(connection.prepareStatement(startsWith("SELECT TABLE_COMMENT")))
        .thenReturn(commentStatement);
    when(commentStatement.executeQuery()).thenReturn(commentResult);
    when(commentResult.next()).thenReturn(true, false);
    when(commentResult.getString("TABLE_COMMENT")).thenReturn("crud probe");
    when(connection.prepareStatement(startsWith("SHOW ALTER TABLE COLUMN")))
        .thenReturn(statusStatement);
    when(statusStatement.executeQuery()).thenReturn(statusResult);

    JdbcTable.Builder tableBuilder = JdbcTable.builder().withComment("OLAP");
    new DorisTableOperations().correctJdbcTableFields(connection, "db", "t", tableBuilder);

    assertEquals("crud probe", tableBuilder.comment());
    verify(commentStatement).setString(1, "db");
    verify(commentStatement).setString(2, "t");
  }

  @Test
  void testEmptyJdbcCommentUsesInformationSchema() throws Exception {
    Connection connection = mock(Connection.class);
    PreparedStatement commentStatement = mock(PreparedStatement.class);
    ResultSet commentResult = mock(ResultSet.class);
    PreparedStatement statusStatement = mock(PreparedStatement.class);
    ResultSet statusResult = mock(ResultSet.class);
    when(connection.prepareStatement(startsWith("SELECT TABLE_COMMENT")))
        .thenReturn(commentStatement);
    when(commentStatement.executeQuery()).thenReturn(commentResult);
    when(commentResult.next()).thenReturn(true, false);
    when(commentResult.getString("TABLE_COMMENT")).thenReturn("crud probe");
    when(connection.prepareStatement(startsWith("SHOW ALTER TABLE COLUMN")))
        .thenReturn(statusStatement);
    when(statusStatement.executeQuery()).thenReturn(statusResult);

    JdbcTable.Builder tableBuilder = JdbcTable.builder();
    new DorisTableOperations().correctJdbcTableFields(connection, "db", "t", tableBuilder);

    assertEquals("crud probe", tableBuilder.comment());
  }

  @Test
  void testValidJdbcCommentNeedsNoFallback() throws Exception {
    Connection connection = mock(Connection.class);
    JdbcTable.Builder tableBuilder = JdbcTable.builder().withComment("crud probe");

    new DorisTableOperations().correctJdbcTableFields(connection, "db", "t", tableBuilder);

    assertEquals("crud probe", tableBuilder.comment());
    verify(connection, never()).prepareStatement(anyString());
  }
}
