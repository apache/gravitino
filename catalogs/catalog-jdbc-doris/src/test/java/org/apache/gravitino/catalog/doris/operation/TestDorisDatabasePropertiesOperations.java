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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.doris.converter.DorisExceptionConverter;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestDorisDatabasePropertiesOperations {

  @ParameterizedTest(name = "{index}")
  @MethodSource("rawPropertyValues")
  void testLoadPreservesRawDatabaseProperties(String value) throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("comment", value);
    properties.put("property1", value);
    properties.put("key\"with\"quotes", value);
    // Doris SHOW CREATE DATABASE uses PrintableMap, which quotes raw metadata without escaping it.
    String showCreateDatabase =
        "CREATE DATABASE `test_database`\nPROPERTIES (\n"
            + properties.entrySet().stream()
                .map(entry -> "\"" + entry.getKey() + "\" = \"" + entry.getValue() + "\"")
                .collect(Collectors.joining(",\n"))
            + "\n)";
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    ResultSet catalogs = mock(ResultSet.class);
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet result = mock(ResultSet.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getCatalogs()).thenReturn(catalogs);
    when(catalogs.next()).thenReturn(true, false);
    when(catalogs.getString("TABLE_CAT")).thenReturn("test_database");
    when(connection.prepareStatement("SHOW CREATE DATABASE `test_database`")).thenReturn(statement);
    when(statement.executeQuery()).thenReturn(result);
    when(result.next()).thenReturn(true, false);
    when(result.getString("Create Database")).thenReturn(showCreateDatabase);
    DorisDatabaseOperations operations = new DorisDatabaseOperations();
    operations.initialize(dataSource, new DorisExceptionConverter(), Collections.emptyMap());

    JdbcSchema schema = operations.load("test_database");

    assertEquals("test_database", schema.name());
    assertEquals(value, schema.comment());
    properties.remove("comment");
    assertEquals(properties, schema.properties());
    verify(result).close();
    verify(statement).close();
  }

  private static Stream<String> rawPropertyValues() {
    return Stream.of(
        "owner's \"comment\" C:\\tmp; DROP DATABASE marker; --",
        "a\" = \"b",
        "a\"\"b",
        "two\\\\backslashes and trailing\\",
        "\\\"quoted\\\"",
        " leading and trailing whitespace ",
        "",
        "long\" = \"value\\\\".repeat(2000));
  }
}
