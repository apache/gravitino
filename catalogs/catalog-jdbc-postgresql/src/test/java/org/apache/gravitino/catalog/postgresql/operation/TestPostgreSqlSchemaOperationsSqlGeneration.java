/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlExceptionConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPostgreSqlSchemaOperationsSqlGeneration {

  @Test
  public void testEscapeCommentInGeneratedSql() {
    PostgreSqlSchemaOperations operations = new PostgreSqlSchemaOperations();

    String sql =
        operations.generateCreateDatabaseSql(
            "test_schema", "owner\\'s comment; DROP SCHEMA marker; --", Collections.emptyMap());

    Assertions.assertEquals(
        "CREATE SCHEMA \"test_schema\";COMMENT ON SCHEMA \"test_schema\" "
            + "IS E'owner\\\\''s comment; DROP SCHEMA marker; --'",
        sql);
  }

  @Test
  void testSchemaNameIsBoundAsData() throws Exception {
    DataSource source = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    ResultSet schemas = mock(ResultSet.class);
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet comments = mock(ResultSet.class);
    String name = "a\\b_%'; SELECT 1; --";
    when(source.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getSearchStringEscape()).thenReturn("\\");
    when(connection.getCatalog()).thenReturn("db");
    when(metadata.getSchemas(eq("db"), anyString())).thenReturn(schemas);
    when(schemas.next()).thenReturn(true, false);
    when(schemas.getString(1)).thenReturn(name);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeQuery()).thenReturn(comments);
    PostgreSqlSchemaOperations operations = new PostgreSqlSchemaOperations();
    operations.initialize(
        source,
        new PostgreSqlExceptionConverter(),
        Collections.singletonMap(JdbcConfig.JDBC_DATABASE.getKey(), "db"));
    Assertions.assertEquals(name, operations.load(name).name());
    verify(metadata).getSchemas("db", "a\\\\b\\_\\%'; SELECT 1; --");
    verify(statement).setString(1, name);
    verify(connection)
        .prepareStatement(
            "SELECT obj_description(n.oid, 'pg_namespace') AS comment\n"
                + "FROM pg_catalog.pg_namespace n\nWHERE n.nspname = ?;\n");
    verify(statement).close();
  }
}
