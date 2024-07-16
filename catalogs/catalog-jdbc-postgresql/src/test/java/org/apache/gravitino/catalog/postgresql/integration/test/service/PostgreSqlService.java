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
package org.apache.gravitino.catalog.postgresql.integration.test.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.integration.test.container.PostgreSQLContainer;
import org.apache.gravitino.integration.test.util.TestDatabaseName;

public class PostgreSqlService {

  private final Connection connection;

  private final String database;

  public PostgreSqlService(PostgreSQLContainer postgreSQLContainer, TestDatabaseName testDBName) {
    String username = postgreSQLContainer.getUsername();
    String password = postgreSQLContainer.getPassword();

    try {
      String jdbcUrl = postgreSQLContainer.getJdbcUrl(testDBName);
      String database = testDBName.toString();
      this.connection = DriverManager.getConnection(jdbcUrl, username, password);
      connection.setCatalog(database);
      this.database = database;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public NameIdentifier[] listSchemas(Namespace namespace) {
    List<String> databases = new ArrayList<>();
    try (PreparedStatement statement =
        connection.prepareStatement(
            "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ?")) {
      statement.setString(1, database);
      ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        String databaseName = resultSet.getString(1);
        databases.add(databaseName);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return databases.stream()
        .map(s -> NameIdentifier.of(ArrayUtils.add(namespace.levels(), s)))
        .toArray(NameIdentifier[]::new);
  }

  public JdbcSchema loadSchema(NameIdentifier schemaIdent) {
    String schema = schemaIdent.name();
    String sql =
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ? AND catalog_name = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, schema);
      statement.setString(2, database);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          throw new NoSuchSchemaException("No such schema: %s", schema);
        }
        String schemaName = resultSet.getString(1);
        return JdbcSchema.builder().withName(schemaName).build();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    try {
      connection.close();
    } catch (SQLException e) {
      // ignore
    }
  }

  public void executeQuery(String sql) {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
